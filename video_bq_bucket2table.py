from google.cloud import bigquery
from google.oauth2 import service_account

def load_csv_to_bigquery(dataset_id, table_id, source_uri, credentials, schema):
    client = bigquery.Client(credentials=credentials)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema
    )
    
    load_job = client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete
    print(f'Loaded {source_uri} into {dataset_id}.{table_id}')

# 설정 값
key_file = 'alookso-347923-6a4fe97bbc31.json'
credentials = service_account.Credentials.from_service_account_file(key_file)
project_id = 'alookso-347923'
bucket_name = 'minju-youtube'
dataset_id = 'minju_dataset'
table_id = 'video_table'

# 기존 테이블의 스키마 정의
schema = [
    bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("duration", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("view_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("like_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("comment_count", "INTEGER", mode="REQUIRED")
]

# GCS 파일 URI 리스트
gcs_uris = [
    f'gs://{bucket_name}/videos/새로운미래_UC58ySWAGaH5AxRQCk3XGLzA_20240607_videos.csv',
    f'gs://{bucket_name}/videos/조국혁신당_UCKsehTG1cZIeb80J4AyiJ6Q_20240607_videos.csv',
    f'gs://{bucket_name}/videos/진보당_UCD2FurCIhOsG3FsTM1-jirA_20240607_videos.csv',
    f'gs://{bucket_name}/videos/개혁신당_UCdkv2W-p3wEK5REQHfu7OKA_20240607_videos.csv',
    f'gs://{bucket_name}/videos/국민의힘_UCGd1rNecfS_MND8PQsKOJhQ_20240607_videos.csv',
    f'gs://{bucket_name}/videos/기본소득당_UCvtJBm9C0rd6Py-GB8KLH1A_20240607_videos.csv',
    f'gs://{bucket_name}/videos/민주당_UCoQD2xsqwzJA93PTIYERokg_20240607_videos.csv',
    f'gs://{bucket_name}/videos/사회민주당_UCHXfJ__xZs-BAA-tU8L19rQ_20240607_videos.csv'
]

# GCS에서 BigQuery로 데이터 로드
for gcs_uri in gcs_uris:
    load_csv_to_bigquery(dataset_id, table_id, gcs_uri, credentials, schema)
    