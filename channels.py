import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime

# Path to your service account key file
KEY_FILE_LOCATION = 'alookso-347923-6a4fe97bbc31.json'

# Scopes required by the YouTube Data API v3
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

# Authenticate using the service account key file
credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION, scopes=SCOPES)

# Initialize the YouTube API client
youtube = build('youtube', 'v3', credentials=credentials)

def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    
    if response['items']:
        channel = response['items'][0]
        channel_info = {
            'channel_id': channel_id,  # Add channel_id
            'title': channel['snippet']['title'],
            # 'description': channel['snippet']['description'],
            'published_at': channel['snippet']['publishedAt'],
            'subscriber_count': channel['statistics'].get('subscriberCount', 'N/A'),
            'view_count': channel['statistics'].get('viewCount', 'N/A'),
            'video_count': channel['statistics'].get('videoCount', 'N/A'),
            'uploads_playlist_id': channel['contentDetails']['relatedPlaylists']['uploads'],
            'crawling_date': datetime.now().strftime('%Y-%m-%d')  # Add crawling date
        }
        return channel_info
    else:
        return None

def save_channel_info_to_csv(party_name, channel_id, channel_info):
    # Ensure the directory exists
    output_dir = 'data/channels'
    os.makedirs(output_dir, exist_ok=True)

    # Get current date
    current_date = datetime.now().strftime('%Y%m%d')

    # Create filename
    output_file = os.path.join(output_dir, f'{party_name}_{channel_id}_{current_date}_channels.csv')

    # Convert channel_info to DataFrame
    df = pd.DataFrame([channel_info])

    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f'Channel info for {party_name} saved to {output_file}')

# List of channel IDs with party names
channel_ids = [
    ('민주당', 'UCoQD2xsqwzJA93PTIYERokg'),
    ('국민의힘', 'UCGd1rNecfS_MND8PQsKOJhQ'),
    ('조국혁신당', 'UCKsehTG1cZIeb80J4AyiJ6Q'),
    ('개혁신당', 'UCdkv2W-p3wEK5REQHfu7OKA'),
    ('진보당', 'UCD2FurCIhOsG3FsTM1-jirA'),
    ('새로운미래', 'UC58ySWAGaH5AxRQCk3XGLzA'),
    ('기본소득당', 'UCvtJBm9C0rd6Py-GB8KLH1A'),
    ('사회민주당', 'UCHXfJ__xZs-BAA-tU8L19rQ')
]

# Fetch and save channel info for each channel
for party_name, channel_id in channel_ids:
    channel_info = get_channel_info(channel_id)
    if channel_info:
        save_channel_info_to_csv(party_name, channel_id, channel_info)
    else:
        print(f"Channel {channel_id} not found.")

