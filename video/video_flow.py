from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect import variables

from video_downloader import download_video
from video_storage import store_video

    
@task(cache_key_fn=task_input_hash)
def transcribe_video(video_file_path: str) -> str:
    logger = get_run_logger()
    logger.info(f'Transcribing video {video_file_path}...')
    return "transcription"

@task(cache_key_fn=task_input_hash)
def index_video_transcription(video_url: str = None, s3_video_file_path: str = None, transcription: str = None):
    logger = get_run_logger()
    logger.info(f'Indexing video {video_url}...')

@flow(retries=2, retry_delay_seconds=60)
def process_video(video_url: str = "youtube.com/watch/123456"):
    logger = get_run_logger()
    logger.info(f'Processing video {video_url}...')
    s3_bucket = variables.get("s3_bucket")
    s3_folder = variables.get("s3_folder")
    downloaded_video = download_video(video_url)
    s3_video_file_path = store_video(
        video_file_path=downloaded_video.video_file, 
        metadata_file_path=downloaded_video.metadata_file, 
        s3_bucket=s3_bucket, 
        s3_folder=s3_folder)
    transcription = transcribe_video(downloaded_video.video_file)
    index_video_transcription(video_url=video_url, s3_video_file_path=s3_video_file_path, transcription=transcription)


if __name__ == "__main__":
    process_video(video_url='https://www.youtube.com/watch?v=P4urfQ1BdGI')
