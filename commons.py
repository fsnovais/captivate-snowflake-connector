import requests
import json
import os

# Define file paths for data storage
current_file_path = os.path.dirname(os.path.abspath(__file__))
download_file_path = os.environ.get('DOWNLOAD_PATH', current_file_path)

file_paths = {
    "user_shows": os.path.join(download_file_path,"user_shows.json"),
    "show_episodes": os.path.join(download_file_path,"show_episodes.json"),
    "shows_daily_hits": os.path.join(download_file_path,"shows_daily_hits.json"),
}

# Function to load show metadata to Snowflake
def load_show_metadata_to_snowflake(cur):
    # Create or replace tables and copy data into Snowflake
    cur.execute(f"create or replace table USER_SHOWS_RAW (dt variant)")
    cur.execute(f"PUT file://{file_paths['user_shows']} @%USER_SHOWS_RAW OVERWRITE = TRUE")
    cur.execute("COPY INTO USER_SHOWS_RAW file_format  =  (format_name = 'SNOWFLAKE_UTIL.CS_JSON_FORMAT')")

    cur.execute("COPY INTO SHOW_HITS_RAW FROM @%SHOW_HITS_RAW FILE_FORMAT = (format_name = 'SNOWFLAKE_UTIL.CS_JSON_FORMAT')")

    cur.execute("COPY INTO SHOW_RAW FROM @%SHOW_RAW FILE_FORMAT = (format_name = 'SNOWFLAKE_UTIL.CS_JSON_FORMAT')")

def load_episode_metadata_to_snowflake(cur):
    cur.execute(f"create or replace table SHOW_EPISODES_RAW (dt variant)")
    cur.execute(f"PUT file://{file_paths['show_episodes']} @%SHOW_EPISODES_RAW OVERWRITE = TRUE")
    cur.execute("COPY INTO SHOW_EPISODES_RAW file_format  =  (format_name = 'SNOWFLAKE_UTIL.CS_JSON_FORMAT')")
    
    cur.execute("COPY INTO EPISODE_HITS_RAW FROM @%EPISODE_HITS_RAW FILE_FORMAT = (format_name = 'SNOWFLAKE_UTIL.CS_JSON_FORMAT')")


# Function to obtain authentication headers
def get_auth(content_secrets):
    # Authenticate and return authorization headers
    response_API = requests.post('https://api.captivate.fm/authenticate/token', data = {"username": content_secrets['USERNAME'], "token": content_secrets['TOKEN']})
    data = response_API.text
    parse_json = json.loads(data)
    bearer = parse_json['user']['token']
    headers = {"authorization": "Bearer " + bearer}
    return(headers)

# Function to retrieve user shows
def get_user_shows(headers, content_secrets):
    # Retrieve user shows data and store it in a file
    user = content_secrets['USERNAME']
    response_show = requests.get(f"https://api.captivate.fm/users/{user}/shows", headers=headers)
    data = response_show.text
    shows_data = json.loads(data)
    with open(file_paths['user_shows'], 'w') as user_shows_fh:
        user_shows_fh.writelines(json.dumps(shows_data, indent=3))

# Function to get a list of user shows
def get_user_show_list(headers, content_secrets):
    # Retrieve a list of user shows
    user = content_secrets['USERNAME']
    response_show = requests.get(f"https://api.captivate.fm/users/{user}/shows", headers=headers)
    data = response_show.text
    shows = []    
    shows_data = json.loads(data)
    for show in shows_data["shows"]:
        shows.append({"id": show["id"], "created_date": show["created"]})
    return(shows)
