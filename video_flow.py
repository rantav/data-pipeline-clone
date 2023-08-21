import httpx
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash)
def download_video(video_url: str) -> str:
    logger = get_run_logger()
    logger.info(f'Downloading video {video_url}...')
    return "video.mp4"

    # response = httpx.get(url, params=params)
    # response.raise_for_status()
    # return response.json()


# @flow
# def get_open_issues(repo_name: str, open_issues_count: int, per_page: int = 100):
#     issues = []
#     pages = range(1, -(open_issues_count // -per_page) + 1)
#     for page in pages:
#         issues.append(
#             get_url.submit(
#                 f"https://api.github.com/repos/{repo_name}/issues",
#                 params={"page": page, "per_page": per_page, "state": "open"},
#             )
#         )
#     return [i for p in issues for i in p.result()]

@task(cache_key_fn=task_input_hash)
def store_video(video_file_path: str) -> str:
    logger = get_run_logger()
    logger.info(f'Storing video {video_file_path}...')
    return 's3://bucket/video.mp4'
    
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
    downloaded_video = download_video(video_url)
    s3_video_file_path = store_video(downloaded_video)
    transcription = transcribe_video(downloaded_video)
    index_video_transcription(video_url=video_url, s3_video_file_path=s3_video_file_path, transcription=transcription)


if __name__ == "__main__":
    process_video()
