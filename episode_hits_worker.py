from datetime import datetime as dt, timedelta, timezone
from dateutil import parser
import requests
import json
import os
import time
from dateutil.parser import isoparse
import asyncio
import aiohttp
from global_common import *
import pytz
from commons import (
    get_auth, 
    get_user_show_list,
    file_paths,
    download_file_path,
    load_episode_metadata_to_snowflake
)


# Function to get show episodes and return a list of episodes
def get_show_episodes(headers, shows):
    # Retrieve and store show episodes data
    episode_data = []
    episode_list = []
    for show in shows:
        id = show["id"]
        response = requests.get(f"https://api.captivate.fm/shows/{id}/episodes", headers=headers)
        response_data = json.loads(response.text)
        episode_data.append({"data":response_data})
        for episode in response_data["episodes"]:
            episode_list.append({"show_id": show["id"], "episode_id": episode["id"], "published_date": episode["published_date"]})
    with open(file_paths['show_episodes'], 'w') as show_episodes_fh:
        show_episodes_fh.writelines(json.dumps(episode_data, indent=3))
    return(episode_list)

async def fetch_episode_data(session, episode, date_range, headers, time_limit):
    show_id = episode["show_id"]
    episode_id = episode["episode_id"]
    response = []

    for start_date, end_date in date_range:
        
        local_timezone = pytz.timezone("Asia/Baku")
        start_date_local = start_date.astimezone(local_timezone)
        end_date_local = end_date.astimezone(local_timezone)

        start_date_formatted = start_date_local.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_formatted = end_date_local.strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            async with session.get(
                f"https://api.captivate.fm/insights/{show_id}/overview/{episode_id}?start={start_date_formatted}&end={end_date_formatted}",
                headers=headers, timeout=60) as response_show:
                if response_show.status == 200:
                    response_data = await response_show.json()
                    response.append(
                        {"id": episode_id, "date": start_date_formatted, "hits": response_data["hits"]})
                if time.time() >= time_limit:
                    print(f"Limit time has taken for episode :{episode_id}")
                    break
        except asyncio.TimeoutError:
            print(f"Timeout error for episode: {episode_id}")
    return response

async def process_episodes(episode_list, headers, download_file_path, cur, conn):
    async with aiohttp.ClientSession() as session:
        utc=pytz.UTC
        tasks = []
        max_concurrent_requests = int(os.getenv("CONCURRENT_EXECUTIONS"))
        time_limit = time.time() + int(os.getenv("TIMEOUT_SECONDS"))
        # Accumulate data from each batch
        all_episode_data = []
        cur.execute("CREATE TABLE IF NOT EXISTS EPISODE_HITS_RAW (dt VARIANT)")
        cur.execute("REMOVE @%EPISODE_HITS_RAW;")
        for i, episode in enumerate(episode_list):
            episode_id = episode["episode_id"]
            start_date = isoparse(episode["published_date"])
            end_date = dt.now(timezone.utc)
            sql = f"""
            select 
                max(date) as latest_date
                from VW_EPISODE_HITS
                where episode_id = '{episode_id}'
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

            task = fetch_episode_data(session, episode, date_range, headers, time_limit)
            tasks.append(task)

            # Limit the number of concurrent tasks
            if len(tasks) >= max_concurrent_requests:
                episode_data = await asyncio.gather(*tasks)
                tasks = []  # Reset the list for the next batch of tasks

                all_episode_data.extend(episode_data)

                if time.time() >= time_limit:
                    print("Finishing process")
                    break

        # Gather the remaining tasks        
        episode_data = await asyncio.gather(*tasks)
        all_episode_data.extend(episode_data)  # Accumulate data
        
        for i, episodes in enumerate(all_episode_data):
            stage_file_name = f"episode_hits_{i}.json"
            path_file = os.path.join(download_file_path, stage_file_name)

            with open(path_file, 'w') as episode_hits_fh:
                episode_hits_fh.write(json.dumps(episodes, indent=3))
            copy_query = f"PUT file://{path_file} @%EPISODE_HITS_RAW/{stage_file_name} OVERWRITE = TRUE"
            cur.execute(copy_query)
        conn.commit()

    
# Function to get episode metrics
def get_episode_metrics(content_secrets, cur, conn): 
    headers = get_auth(content_secrets)
    shows = get_user_show_list(headers, content_secrets)
    episode_list = get_show_episodes(headers, shows)
    asyncio.run(process_episodes(episode_list, headers, download_file_path, cur, conn))
    

def lambda_handler(event, context):
    # Initialize variables and connection
    done = False
    retries = 0
    max_retries = 2
    meta_data_api_processed = False
    snowflake_secrets = get_secret_by_name(os.environ['SNOWFLAKE_SECRET'])
    content_secrets = get_secret_by_name(os.environ['CAPTIVATE_SECRET'])
    conn = get_snowflake_connection(snowflake_secrets)
    cur = conn.cursor()
    while True:
        try:
            print(f"Started to get episode metrics")
            if not meta_data_api_processed:
                get_episode_metrics(content_secrets,cur,conn)
                meta_data_api_processed = True
            print(f"Finished to get episode metrics")
            print(f"Started to load episode metrics to snowflake")
            load_episode_metadata_to_snowflake(cur)
            print(f"Finished to load episode metrics to snowflake")

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