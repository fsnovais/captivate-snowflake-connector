from datetime import datetime as dt, timedelta, timezone
from dateutil import parser
import requests
import json
import os
from dateutil.parser import isoparse
from global_common import *
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


# Function to get episodes by day and store them in Snowflake
def get_episodes_by_day(headers,episode_list, conn, cur):
    episode_data = []
    for episode in episode_list:
    # Retrieve and process episodes data
        show_id = episode["show_id"]
        episode_id = episode["episode_id"]
        data = {
        "start": episode['published_date'],
        "end": dt.now().isoformat(),
        "interval": "1d",
        "timezone": "UTC",
        "countryCode": None,
        "types": ["byLocation","byUserAgentBrowser", "byUserAgentDevice","byUserAgentOs"]
        }   
        response = requests.post(f"https://api.captivate.fm/insights/{show_id}/range/{episode_id}",json=data, headers=headers)
        if(response.status_code) == 200:
            response_data = json.loads(response.text)
            episode_data.append({"episode": episode_id, "data":response_data})
    try:
    
        # Create or replace the table EPISODES_RAW (if needed)
        cur.execute("CREATE OR REPLACE TABLE EPISODES_RAW (dt VARIANT)")
    
        # Retrieve and flatten JSON data
        flattened_json_data = [json.dumps(obj, indent=3) for obj in episode_data]

        # Loop through the flattened JSON data and upload to the Snowflake stage
        for i, obj in enumerate(flattened_json_data):
            stage_file_name = f'episode_file_{i}.json'
            path_file = os.path.join(download_file_path, stage_file_name)

            with open(path_file, 'w') as episode_by_day_fh:
                episode_by_day_fh.write(obj)

            # PUT the file to the Snowflake stage
            copy_query = f"PUT file://{path_file} @%EPISODES_RAW OVERWRITE = TRUE"
            cur.execute(copy_query)

        # Copy the data from the stage into the table
        cur.execute("COPY INTO EPISODES_RAW FROM @%EPISODES_RAW FILE_FORMAT = (format_name = 'SNOWFLAKE_UTIL.CS_JSON_FORMAT')")
        # Commit the changes
        conn.commit()
    except Exception as e:
        print("Error:", e)
    finally:
        print('done')

    
# Function to get episode metrics
def get_episode_metrics(content_secrets, cur, conn): 
    headers = get_auth(content_secrets)
    shows = get_user_show_list(headers, content_secrets)
    episode_list = get_show_episodes(headers, shows)
    get_episodes_by_day(headers, episode_list, conn, cur)
    

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