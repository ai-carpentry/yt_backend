import os
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
import isodate
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Retrieve the API key from environment variables
API_KEY = os.getenv('YOUTUBE_APIKEY')

# Initialize YouTube API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Function to convert ISO 8601 duration to human-readable format
def convert_duration(duration):
    td = isodate.parse_duration(duration)
    minutes, seconds = divmod(td.total_seconds(), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

def get_channel_videos(channel_id):
    # Get upload playlist id
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    upload_playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # Fetch video details
    videos = []
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=upload_playlist_id, part='snippet', maxResults=50, pageToken=next_page_token).execute()
        video_ids = [item['snippet']['resourceId']['videoId'] for item in res['items']]
        
        video_details_res = youtube.videos().list(id=','.join(video_ids), part='snippet,contentDetails,statistics').execute()
        for item in video_details_res['items']:
            video_id = item['id']
            video_title = item['snippet']['title']
            video_published_at = item['snippet']['publishedAt']
            video_duration = convert_duration(item['contentDetails']['duration'])
            video_view_count = item['statistics'].get('viewCount', 0)
            video_like_count = item['statistics'].get('likeCount', 0)
            video_comment_count = item['statistics'].get('commentCount', 0)
            
            videos.append({
                'channel_id': channel_id,
                'video_id': video_id,
                'title': video_title,
                'published_at': video_published_at,
                'duration': video_duration,
                'view_count': video_view_count,
                'like_count': video_like_count,
                'comment_count': video_comment_count
            })
        
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break

    return videos

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

# Ensure the directory exists
output_dir = 'data/videos'
os.makedirs(output_dir, exist_ok=True)

# Fetch video details for each channel and save to CSV
for party_name, channel_id in channel_ids:
    videos = get_channel_videos(channel_id)
    df = pd.DataFrame(videos)
    
    # Get current date
    current_date = datetime.now().strftime('%Y%m%d')
    
    # Create filename
    output_file = os.path.join(output_dir, f'{party_name}_{channel_id}_{current_date}_videos.csv')
    df.to_csv(output_file, index=False)
    
    print(f'Video details for {party_name} saved to {output_file}')

