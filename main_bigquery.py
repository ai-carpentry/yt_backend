import os
from google.cloud import bigquery
from google.oauth2 import service_account

# 설정 값
key_file = 'alookso-347923-6a4fe97bbc31.json'  # 올바른 서비스 계정 키 파일 경로
project_id = 'alookso-347923'
bucket_name = 'minju-youtube'
dataset_id = 'minju_dataset'
channels_combined_csv = 'channels_combined.csv'
videos_combined_csv = 'videos_combined.csv'

# 인증 설정
credentials = service_account.Credentials.from_service_account_file(key_file)
bigquery_client = bigquery.Client(credentials=credentials, project=project_id)

def load_csv_to_bigquery(dataset_id, table_id, source_uri, schema):
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    load_job = bigquery_client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete
    print(f'Loaded {source_uri} into {dataset_id}.{table_id}')

# channel_table 스키마 정의
channel_table_schema = [
    bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    # bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("subscriber_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("view_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("video_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("uploads_playlist_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("crawling_date", "DATE", mode="REQUIRED")
]

# video_table 스키마 정의
video_table_schema = [
    bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("duration", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("view_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("like_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("comment_count", "INTEGER", mode="REQUIRED")
]

# GCS 파일 경로
channels_combined_uri = f'gs://{bucket_name}/{channels_combined_csv}'
videos_combined_uri = f'gs://{bucket_name}/{videos_combined_csv}'

# BigQuery 테이블에 데이터 로드
load_csv_to_bigquery(dataset_id, 'channel_table', channels_combined_uri, channel_table_schema)
load_csv_to_bigquery(dataset_id, 'video_table', videos_combined_uri, video_table_schema)
