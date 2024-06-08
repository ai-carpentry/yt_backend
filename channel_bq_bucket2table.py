from google.cloud import bigquery
from google.oauth2 import service_account

def load_csv_to_bigquery(dataset_id, table_id, source_uri, credentials, schema):
    client = bigquery.Client(credentials=credentials)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        allow_jagged_rows=True  # Allow missing trailing columns
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
table_id = 'channel_table'

# 기존 테이블의 스키마 정의
schema = [
    bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    # bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("published_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("subscriber_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("view_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("video_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("uploads_playlist_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("crawling_date", "DATE", mode="REQUIRED")  # Make crawling_date nullable
]

# GCS 파일 URI 리스트
gcs_uris = [
    f'gs://{bucket_name}/channels/새로운미래_UC58ySWAGaH5AxRQCk3XGLzA_20240608_channels.csv',
    f'gs://{bucket_name}/channels/조국혁신당_UCKsehTG1cZIeb80J4AyiJ6Q_20240608_channels.csv',
    f'gs://{bucket_name}/channels/진보당_UCD2FurCIhOsG3FsTM1-jirA_20240608_channels.csv',
    f'gs://{bucket_name}/channels/개혁신당_UCdkv2W-p3wEK5REQHfu7OKA_20240608_channels.csv',
    f'gs://{bucket_name}/channels/국민의힘_UCGd1rNecfS_MND8PQsKOJhQ_20240608_channels.csv',
    f'gs://{bucket_name}/channels/기본소득당_UCvtJBm9C0rd6Py-GB8KLH1A_20240608_channels.csv',
    f'gs://{bucket_name}/channels/민주당_UCoQD2xsqwzJA93PTIYERokg_20240608_channels.csv',
    f'gs://{bucket_name}/channels/사회민주당_UCHXfJ__xZs-BAA-tU8L19rQ_20240608_channels.csv'
]

# GCS에서 BigQuery로 데이터 로드
for gcs_uri in gcs_uris:
    load_csv_to_bigquery(dataset_id, table_id, gcs_uri, credentials, schema)
    
