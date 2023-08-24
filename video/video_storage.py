import os

from prefect import flow, task, get_run_logger, variables
from prefect.tasks import task_input_hash
from prefect_aws import AwsCredentials
from prefect_gcp import GcpCredentials

from google.cloud import storage

import boto3

@flow
def store_video(video_file_path: str, metadata_file_path: str, s3_bucket: str, s3_folder: str) -> str:
    logger = get_run_logger()
    logger.info(f'Storing video (and metadata) {video_file_path}...')
    
    video_s3_path = s3_upload_file.submit(video_file_path, s3_bucket, s3_folder)
    metadata_s3_path = s3_upload_file.submit(metadata_file_path, s3_bucket, s3_folder)
    metadata_s3_path.result()
    ret_path = f's3://{s3_bucket}/{video_s3_path.result()}'
    return ret_path

@task(cache_key_fn=task_input_hash)
def s3_upload_file(file_path: str, s3_bucket: str, s3_folder: str) -> str:
    logger = get_run_logger()
    creds = AwsCredentials.load("aws-credentials")

    s3 = boto3.client('s3', aws_access_key_id=creds.aws_access_key_id,
                            aws_secret_access_key=creds.aws_secret_access_key.get_secret_value())
    
    s3_path =  os.path.join(s3_folder, file_path.split('/')[-1])
    s3.upload_file(file_path, s3_bucket, s3_path)
    logger.info(f'Upload Successful to {s3_path}')

    return s3_path

@task(cache_key_fn=task_input_hash)
def gcs_upload_file(local_file_path: str, gcs_bucket: str, gcs_folder: str) -> str:
    logger = get_run_logger()
    creds = GcpCredentials.load("gcp-credentials")
    credentials_dict = creds.service_account_info.get_secret_value()
    storage_client = storage.Client.from_service_account_info(credentials_dict)

    # Get the bucket
    bucket = storage_client.bucket(gcs_bucket)

    # Specify the source file and destination blob name
    gcs_destination = os.path.join(gcs_folder, local_file_path.split('/')[-1])
    blob = bucket.blob(gcs_destination)

    # Upload the file
    blob.upload_from_filename(local_file_path)
    ret_path = f'gs://{gcs_bucket}/{gcs_destination}'
    return ret_path

def s3_download_file(s3_bucket: str, s3_file_path: str, local_directory: str) -> str:
    logger = get_run_logger()
    creds = AwsCredentials.load("aws-credentials")

    s3 = boto3.client('s3', aws_access_key_id=creds.aws_access_key_id,
                            aws_secret_access_key=creds.aws_secret_access_key.get_secret_value())
    
    local_file_path = os.path.join(local_directory, s3_file_path.split('/')[-1])
    s3.download_file(s3_bucket, s3_file_path, local_file_path)
    logger.info(f'Download Successful to {local_file_path}')

    return local_file_path

if __name__ == '__main__':
    metadata_file_path = 'video_downloads/חי פה - חדשות חיפה： הפגנה ספונטנית בחורב בעקבות התפטרותו של ניצב עמי אשד (צילום： מטה מחאת העם) [P4urfQ1BdGI].info.json'
    # video_file_path = 'video_downloads/חי פה - חדשות חיפה： הפגנה ספונטנית בחורב בעקבות התפטרותו של ניצב עמי אשד (צילום： מטה מחאת העם) [P4urfQ1BdGI].mp4'
    video_file_path = '/Users/rantav/Downloads/tmp-whisper/audio_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3'
    s3_bucket = variables.get('s3_bucket', 'data-pipeline-video-downloads')
    s3_folder = variables.get('s3_folder', 'local-test/video_downloads')


    result = store_video(video_file_path, metadata_file_path, s3_bucket, s3_folder)

    print(result)