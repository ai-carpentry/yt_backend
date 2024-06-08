import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from google.cloud import storage

# Path to your service account key file
KEY_FILE_LOCATION = 'alookso-347923-6a4fe97bbc31.json'

# Scopes required by the YouTube Data API v3
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

# Authenticate using the service account key file
credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION, scopes=SCOPES)

# Initialize the YouTube API client
youtube = build('youtube', 'v3', credentials=credentials)

# Initialize Google Cloud Storage client
storage_client = storage.Client(credentials=credentials)
bucket_name = 'minju-youtube'
bucket = storage_client.bucket(bucket_name)

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

def save_channel_info_to_gcs(party_name, channel_id, channel_info):
    # Get current date
    current_date = datetime.now().strftime('%Y%m%d')

    # Create filename
    file_name = f'{party_name}_{channel_id}_{current_date}_channels.csv'

    # Convert channel_info to DataFrame
    df = pd.DataFrame([channel_info])

    # Save DataFrame to a CSV file in memory
    csv_data = df.to_csv(index=False)

    # Upload the CSV file to GCS
    blob = bucket.blob(f'channels/{file_name}')
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f'Channel info for {party_name} saved to gs://{bucket_name}/channels/{file_name}')

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
        save_channel_info_to_gcs(party_name, channel_id, channel_info)
    else:
        print(f"Channel {channel_id} not found.")
