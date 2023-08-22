import os
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect_aws import AwsCredentials

import boto3
from botocore.exceptions import NoCredentialsError

@flow
def store_video(video_file_path: str, metadata_file_path: str, s3_bucket: str, s3_folder: str) -> str:
    logger = get_run_logger()
    logger.info(f'Storing video (and metadata) {video_file_path}...')
    
    video_s3_path = upload_file.submit(video_file_path, s3_bucket, s3_folder)
    metadata_s3_path = upload_file.submit(metadata_file_path, s3_bucket, s3_folder)
    metadata_s3_path.result()
    return video_s3_path.result()

@task(cache_key_fn=task_input_hash)
def upload_file(file_path: str, s3_bucket: str, s3_folder: str) -> str:
    logger = get_run_logger()
    creds = AwsCredentials.load("aws-credentials")

    s3 = boto3.client('s3', aws_access_key_id=creds.aws_access_key_id,
                            aws_secret_access_key=creds.aws_secret_access_key.get_secret_value())
    
    s3_path =  os.path.join(s3_folder, file_path.split('/')[-1])
    try:
        s3.upload_file(file_path, s3_bucket, s3_path)
        logger.info(f'Upload Successful to {s3_path}')
    except FileNotFoundError as e:
        # We keep these here just so to remember what could happen
        raise e
    except NoCredentialsError as e:
        raise e

    return s3_path

if __name__ == '__main__':
    metadata_file_path = 'video_downloads/חי פה - חדשות חיפה： הפגנה ספונטנית בחורב בעקבות התפטרותו של ניצב עמי אשד (צילום： מטה מחאת העם) [P4urfQ1BdGI].info.json'
    video_file_path = 'video_downloads/חי פה - חדשות חיפה： הפגנה ספונטנית בחורב בעקבות התפטרותו של ניצב עמי אשד (צילום： מטה מחאת העם) [P4urfQ1BdGI].mp4'
    s3_bucket = 'data-pipeline-video-downloads'
    s3_folder = 'local-test/video_downloads'

    result = store_video(video_file_path, metadata_file_path, s3_bucket, s3_folder)
    # print(result)