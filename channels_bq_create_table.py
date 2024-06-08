from google.cloud import bigquery
from google.oauth2 import service_account

def create_dataset_and_table(key_file, project_id, dataset_id, table_id, schema):
    credentials = service_account.Credentials.from_service_account_file(key_file)
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    # 데이터셋 참조
    dataset_ref = client.dataset(dataset_id)
    
    # 데이터셋이 존재하는지 확인하고 없으면 생성
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception as e:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast3"  # 데이터셋의 위치 설정
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")
    
    # 테이블 생성
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"Table {table_id} created.")
    except Exception as e:
        print(f"Table {table_id} already exists or error occurred: {e}")

# 설정 값
key_file = 'alookso-347923-6a4fe97bbc31.json'
project_id = 'alookso-347923'
dataset_id = 'minju_dataset'
table_id = 'channel_table'

# 테이블 스키마 정의
schema = [
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

create_dataset_and_table(key_file, project_id, dataset_id, table_id, schema)


# 데이블 생성 확인 함수 ------------------
def check_dataset_and_table(key_file, project_id, dataset_id, table_id):
    credentials = service_account.Credentials.from_service_account_file(key_file)
    client = bigquery.Client(credentials=credentials, project=project_id)

    # 데이터셋 확인
    dataset_ref = client.dataset(dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"Dataset {dataset.dataset_id} found in location {dataset.location}.")
    except Exception as e:
        print(f"Dataset {dataset_id} not found: {e}")

    # 테이블 확인
    table_ref = dataset_ref.table(table_id)
    try:
        table = client.get_table(table_ref)
        print(f"Table {table.table_id} found in dataset {dataset_id}.")
    except Exception as e:
        print(f"Table {table_id} not found: {e}")

# 설정 값
key_file = 'alookso-347923-6a4fe97bbc31.json'
project_id = 'alookso-347923'
dataset_id = 'minju_dataset'
table_id = 'channel_table'

# 데이터셋과 테이블 확인
check_dataset_and_table(key_file, project_id, dataset_id, table_id)
