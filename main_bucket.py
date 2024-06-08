#################################################
# 파일명: main_bucket.py
# 목적: GCS 버킷 최신 CSV 파일들을 병합하고, 병합된 파일을 GCS에 업로드
# 기능: 
#   1. GCS minju-youtube 버킷에서 최신 CSV 파일들을 다운로드
#   2. 파일들을 병합하여 하나의 CSV 파일로 생성
#   3. 병합된 파일을 다시 minju-youtube GCS에 업로드
#################################################

import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime

# 설정 값
key_file = 'alookso-347923-6a4fe97bbc31.json'  # 올바른 서비스 계정 키 파일 경로
project_id = 'alookso-347923'
bucket_name = 'minju-youtube'
local_tmp_dir = 'tmp'  # 로컬 프로젝트의 tmp 폴더

# tmp 디렉토리 생성
os.makedirs(local_tmp_dir, exist_ok=True)

# 인증 설정
credentials = service_account.Credentials.from_service_account_file(key_file)
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket(bucket_name)

def get_latest_blobs(prefix):
    blobs = list(bucket.list_blobs(prefix=prefix))
    latest_blobs = {}
    
    for blob in blobs:
        channel_id = blob.name.split('_')[1]
        if channel_id not in latest_blobs or blob.time_created > latest_blobs[channel_id].time_created:
            latest_blobs[channel_id] = blob
            
    return list(latest_blobs.values())

def download_and_combine_csv(prefix, combined_file_name):
    latest_blobs = get_latest_blobs(prefix)
    
    combined_df = pd.DataFrame()
    
    for blob in latest_blobs:
        temp_file_path = os.path.join(local_tmp_dir, os.path.basename(blob.name))
        blob.download_to_filename(temp_file_path)
        df = pd.read_csv(temp_file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    combined_csv_path = os.path.join(local_tmp_dir, combined_file_name)
    combined_df.to_csv(combined_csv_path, index=False)
    
    return combined_csv_path

def upload_file_to_gcs(local_file_path, gcs_file_path):
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(local_file_path)
    print(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_file_path}")

# 채널 CSV 파일 병합 및 업로드
channels_combined_csv = download_and_combine_csv('channels/', 'channels_combined.csv')
upload_file_to_gcs(channels_combined_csv, 'channels_combined.csv')

# 비디오 CSV 파일 병합 및 업로드
videos_combined_csv = download_and_combine_csv('videos/', 'videos_combined.csv')
upload_file_to_gcs(videos_combined_csv, 'videos_combined.csv')


