from datetime import datetime as dt, timedelta, timezone
from dateutil import parser
import requests
import json
import os
import asyncio
import aiohttp
import time
import pytz
from commons import (
    get_auth, 
    get_user_shows, 
    get_user_show_list,
    download_file_path,
    load_show_metadata_to_snowflake
)

async def fetch_show_data(session, show, date_range, headers, time_limit):
    show_id = show["id"]
    response = []

    for start_date, end_date in date_range:
        
        local_timezone = pytz.timezone('Antarctica/Mawson')
        start_date_local = start_date.astimezone(local_timezone)
        end_date_local = end_date.astimezone(local_timezone)

        start_date_formatted = start_date_local.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_formatted = end_date_local.strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            async with session.get(
                f"https://api.captivate.fm/insights/{show_id}/overview?start={start_date_formatted}&end={end_date_formatted}", headers=headers, timeout=60) as response_show:
                if response_show.status == 200:
                    response_data = await response_show.json()
                    response.append({"id": show_id, "date": start_date_formatted, "hits": response_data["hits"]})
                if time.time() >= time_limit:
                    print(f"Limit time has taken for show :{show_id}")
                    break
        except asyncio.TimeoutError:
            print(f"Timeout error for show: {show_id}")
    return response

async def process_shows(shows, headers, download_file_path, cur, conn):
    async with aiohttp.ClientSession() as session:
        utc=pytz.UTC
        tasks = []
        max_concurrent_requests = int(os.getenv("CONCURRENT_EXECUTIONS"))
        time_limit = time.time() + int(os.getenv("TIMEOUT_SECONDS"))
        # Accumulate data from each batch
        all_show_data = []
        cur.execute("CREATE TABLE IF NOT EXISTS SHOW_HITS_RAW (dt VARIANT)")
        cur.execute("REMOVE @%SHOW_HITS_RAW;")
        for i, show in enumerate(shows):
            show_id = show["id"]
            start_date = parser.parse(show["created_date"])
            end_date = dt.now(timezone.utc)
            sql = f"""
            select 
                max(date) as latest_date
                from VW_SHOW_HITS
                where show_id = '{show_id}'
            """
            try:
                cur.execute(sql)
                snow_data = cur.fetchall()
            except Exception as e:
                print(f"Error while getting lastest date and error is {e}")
                snow_data = [(None,)]
            pass
            if (snow_data[0][0] != None):
                latest_date = utc.localize(dt.combine(snow_data[0][0], dt.min.time()))
                date_value = latest_date.astimezone(pytz.UTC)
            else:
                date_value = start_date
            date_range = [
                ((date_value + timedelta(days=n)).replace(hour=0, minute=0, second=0, microsecond=0),
                (date_value + timedelta(days=n + 1)).replace(hour=0, minute=0, second=0, microsecond=0))
                for n in range((end_date - date_value).days)]

            task = fetch_show_data(session, show, date_range, headers, time_limit)
            tasks.append(task)

            # Limit the number of concurrent tasks
            if len(tasks) >= max_concurrent_requests:
                show_data = await asyncio.gather(*tasks)
                tasks = []  # Reset the list for the next batch of tasks

                all_show_data.extend(show_data)

                if time.time() >= time_limit:
                    print("Finishing process")
                    break

        # Gather the remaining tasks        
        show_data = await asyncio.gather(*tasks)
        all_show_data.extend(show_data)  # Accumulate data
        
        for i, show in enumerate(all_show_data):
            stage_file_name = f"show_hits_{i}.json"
            path_file = os.path.join(download_file_path, stage_file_name)

            with open(path_file, 'w') as show_hits_fh:
                show_hits_fh.write(json.dumps(show, indent=3))
            copy_query = f"PUT file://{path_file} @%SHOW_HITS_RAW/{stage_file_name} OVERWRITE = TRUE"
            cur.execute(copy_query)
        conn.commit()


# Function to get show-by-day data
def get_show_by_day(headers,shows, cur, conn):
    # Retrieve and store show-by-day data
    show_data = []
    cur.execute("CREATE TABLE IF NOT EXISTS SHOW_RAW (dt VARIANT)")
    cur.execute("REMOVE @%SHOW_RAW;")
    for show in shows:
        id = show["id"]
        data = {
 		"start": show['created_date'],
        "end": dt.now().isoformat(),
        "interval": "1d",
        "timezone": "UTC",
        "countryCode": None,
        "types": ["byLocation","byUserAgentBrowser", "byUserAgentDevice","byUserAgentOs"]
    }    
        response = requests.post(f"https://api.captivate.fm/insights/{id}/range",json=data, headers=headers)
        if response.status_code == 200:
            response_data = json.loads(response.text)
            show_data.append({"show": show, "data":response_data})
    
    for i, show in enumerate(show_data):
        stage_file_name = f"show_{i}.json"
        path_file = os.path.join(download_file_path, stage_file_name)

        with open(path_file, 'w') as show_by_day_fh:
            show_by_day_fh.write(json.dumps(show, indent=3))
        copy_query = f"PUT file://{path_file} @%SHOW_RAW/{stage_file_name} OVERWRITE = TRUE"
        cur.execute(copy_query)
    conn.commit()

# Function to get show metrics
def get_show_metrics(content_secrets, cur, conn): 
    headers = get_auth(content_secrets)
    get_user_shows(headers, content_secrets)
    shows = get_user_show_list(headers, content_secrets)
    get_show_by_day(headers, shows, cur, conn)
    asyncio.run(process_shows(shows, headers, download_file_path, cur, conn))

def lambda_handler(event, context):
    # Initialize variables and connection
    done = False
    retries = 0
    max_retries = 2
    meta_data_api_processed = False
    snowflake_secrets = os.environ['SNOWFLAKE_SECRET']
    content_secrets = os.environ['CAPTIVATE_SECRET']
    conn = snowflake_secrets
    cur = conn.cursor()
    while True:
        try:
            print(f"Started to get show metrics")
            if not meta_data_api_processed:
                get_show_metrics(content_secrets, cur, conn)
                meta_data_api_processed = True
            print(f"Finished to get show metrics")
            print(f"Started to load show metrics to snowflake")
            load_show_metadata_to_snowflake(cur)
            print(f"Finished to load show metrics to snowflake")

        except Exception as e:
            print(f"Error while executing lambda and error is {e}")
        else:
            done = True
        finally:
            retries = retries + 1

        if done:
            break
        if retries > max_retries:
            break

    if conn:
        conn.close()
    if done:
        print(f"Finished to get content show metrics using api")
        return "Done"
    else:
        raise Exception("There was an issue while getting show metrics data even after 2 tries. Please check all the logs for more details on errors.")