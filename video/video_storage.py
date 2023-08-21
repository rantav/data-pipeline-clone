import os
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect_aws import AwsCredentials

import boto3
from botocore.exceptions import NoCredentialsError

@task(cache_key_fn=task_input_hash)
def store_video(video_file_path: str, metadata_file_path: str, s3_bucket: str, s3_folder: str) -> str:
    logger = get_run_logger()
    logger.info(f'Storing video {video_file_path}...')
    
    # Store the video in S3
    aws_credentials_block = AwsCredentials.load("aws-credentials")

    s3 = boto3.client('s3', aws_access_key_id=aws_credentials_block.aws_access_key_id,
                            aws_secret_access_key=aws_credentials_block.aws_secret_access_key.get_secret_value())
    
    metadata_s3_path =  os.path.join(s3_folder, metadata_file_path.split('/')[-1])
    video_s3_path =  os.path.join(s3_folder, video_file_path.split('/')[-1])
    try:
        s3.upload_file(metadata_file_path, s3_bucket, metadata_s3_path)
        # s3.upload_file(video_file_path, s3_bucket, video_s3_path)
        logger.info("Upload Successful")
    except FileNotFoundError as e:
        logger.error("The file was not found")
        raise e
    except NoCredentialsError as e:
        logger.error("Credentials not available")
        raise e

    
    return video_s3_path

@flow
def store_video_flow(video_file_path: str, metadata_file_path: str, s3_bucket: str, s3_folder: str) -> str:
    return store_video(video_file_path, metadata_file_path, s3_bucket, s3_folder)

if __name__ == '__main__':
    metadata_file_path = 'video_downloads/חי פה - חדשות חיפה： הפגנה ספונטנית בחורב בעקבות התפטרותו של ניצב עמי אשד (צילום： מטה מחאת העם) [P4urfQ1BdGI].info.json'
    video_file_path = 'video_downloads/חי פה - חדשות חיפה： הפגנה ספונטנית בחורב בעקבות התפטרותו של ניצב עמי אשד (צילום： מטה מחאת העם) [P4urfQ1BdGI].mp4'
    s3_bucket = 'data-pipeline-video-downloads'
    s3_folder = 'local-test/video_downloads'

    result = store_video_flow(video_file_path, metadata_file_path, s3_bucket, s3_folder)
    # print(result)