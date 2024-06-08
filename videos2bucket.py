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
        blob_name = f"videos/{file_path.split('/')[-1]}"
        
        # 버킷에 Blob 객체 생성
        blob = bucket.blob(blob_name)
        
        # 파일 업로드
        blob.upload_from_filename(file_path)
        
        print(f'File {file_path} uploaded to {bucket_name}.')

# 업로드할 파일 경로 리스트
file_paths = [
    'data/videos/새로운미래_UC58ySWAGaH5AxRQCk3XGLzA_20240607_videos.csv',
    'data/videos/조국혁신당_UCKsehTG1cZIeb80J4AyiJ6Q_20240607_videos.csv',
    'data/videos/진보당_UCD2FurCIhOsG3FsTM1-jirA_20240607_videos.csv',
    'data/videos/개혁신당_UCdkv2W-p3wEK5REQHfu7OKA_20240607_videos.csv',
    'data/videos/국민의힘_UCGd1rNecfS_MND8PQsKOJhQ_20240607_videos.csv',
    'data/videos/기본소득당_UCvtJBm9C0rd6Py-GB8KLH1A_20240607_videos.csv',
    'data/videos/민주당_UCoQD2xsqwzJA93PTIYERokg_20240607_videos.csv',
    'data/videos/사회민주당_UCHXfJ__xZs-BAA-tU8L19rQ_20240607_videos.csv'    
]

# 버킷 이름
bucket_name = 'minju-youtube'

# 서비스 계정 키 파일 경로
key_file = 'alookso-347923-6a4fe97bbc31.json'

# 파일 업로드 함수 호출
upload_files_to_bucket(bucket_name, file_paths, key_file)
