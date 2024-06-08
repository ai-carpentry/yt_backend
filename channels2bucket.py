import os
from google.cloud import storage
from google.oauth2 import service_account

def upload_files_to_bucket(bucket_name, file_paths, key_file):
    # 서비스 계정 키 파일을 사용하여 인증 자격 증명 생성
    credentials = service_account.Credentials.from_service_account_file(key_file)

    # Google Cloud Storage 클라이언트 초기화
    client = storage.Client(credentials=credentials)

    # 버킷 객체 가져오기
    bucket = client.get_bucket(bucket_name)

    # 파일 업로드
    for file_path in file_paths:
        # 업로드할 파일 이름 설정
        blob_name = f"channels/{os.path.basename(file_path)}"
        
        # 버킷에 Blob 객체 생성
        blob = bucket.blob(blob_name)
        
        # 파일 업로드
        blob.upload_from_filename(file_path)
        
        print(f'File {file_path} uploaded to {bucket_name}/{blob_name}.')

# data/channels 폴더의 모든 CSV 파일 경로 리스트 생성
channel_folder = 'data/channels'
file_paths = [os.path.join(channel_folder, file) for file in os.listdir(channel_folder) if file.endswith('.csv')]

# 버킷 이름
bucket_name = 'minju-youtube'

# 서비스 계정 키 파일 경로
key_file = 'alookso-347923-6a4fe97bbc31.json'

# 파일 업로드 함수 호출
upload_files_to_bucket(bucket_name, file_paths, key_file)
