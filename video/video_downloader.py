import json
from typing import Optional
from pydantic import BaseModel
from prefect import flow, task, get_run_logger, variables
from prefect.tasks import task_input_hash

import yt_dlp as youtube_dl

class VideoDownloadResult(BaseModel):
    video_file: str
    metadata_file: str
    ydl_format: str
    id: str
    title: str
    formats: list[dict]
    thumbnails: list[dict]
    thumbnail: str
    description: str
    channel_id: str
    channel_url: str
    channel: str
    channel_follower_count: int
    duration: int
    view_count: int
    average_rating: Optional[float]
    webpage_url: str
    release_timestamp: Optional[int]
    subtitles: dict
    comment_count: int
    like_count: int
    uploader: str
    uploader_id: str
    uploader_url: str
    upload_date: int
    fulltitle: str
    duration_string: str
    format: str
    format_id: str
    ext: str
    language: str
    filesize_approx: int
    audio_channels: int

@flow
def download_video_flow(video_url: str) -> VideoDownloadResult:
    return download_video(video_url)

@task(cache_key_fn=task_input_hash)
def download_video(video_url: str) -> VideoDownloadResult:
    logger = get_run_logger()
    video_type = select_video_type(video_url)
    logger.info(f'Downloading {video_type} video {video_url}...')
    if video_type == 'youtube':
        result = download_youtube_video(video_url)

    return result

def select_video_type(video_url: str) -> str:
    if 'youtube' in video_url:
        return 'youtube'
    else:
        return 'unknown'
    
def download_youtube_video(youtube_url) -> VideoDownloadResult:
    logger = get_run_logger()
    ydl_format = 'worstvideo[ext=mp4]+bestaudio[ext=m4a]/mp4'
    local_working_dir = variables.get('local_working_dir')
    ydl_opts = {
        # 'logger': logger, # I want to use the prefect logger but when I try to, logging isn't working :-(
        # 'outtmpl': output_filename,
        # 'writeinfojson': True, # This doesn't work so I write the file manually below
        'paths': {'home': './video_downloads'},
        'format': ydl_format,
        'retries': 3,
        # 'merge_output_format': 'mp4',
        # 'nocheckcertificate': True,
        # 'ignoreerrors': True,
        # 'no_warnings': True,
        # 'quiet': True,
        # 'nooverwrites': True,
        # 'prefer_ffmpeg': True,
        # 'postprocessors': [{
        #     'key': 'FFmpegVideoConvertor',
        #     'preferedformat': 'mp4'
        # }],
        # 'progress_hooks': [lambda d: print(d['status'])]
    }

    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        try:
            video_info = ydl.extract_info(youtube_url, download=True)
            logger.info(f"Download success for {youtube_url} (Title: {video_info['title']})")
            video_file = ydl.prepare_filename(video_info) # youtube-dl automatically adds the correct extension
            metadata_file = video_file.replace('.mp4', '.info.json')
            result = VideoDownloadResult(
                video_file=video_file, 
                metadata_file=metadata_file,
                ydl_format=ydl_format,
                id=video_info['id'], 
                title=video_info['title'], 
                formats=video_info['formats'], 
                thumbnails=video_info['thumbnails'], 
                thumbnail=video_info['thumbnail'], 
                description=video_info['description'], 
                channel_id=video_info['channel_id'], 
                channel_url=video_info['channel_url'],
                channel=video_info['channel'], 
                channel_follower_count=video_info['channel_follower_count'], 
                duration=video_info['duration'], 
                view_count=video_info['view_count'], 
                average_rating=video_info['average_rating'], 
                webpage_url=video_info['webpage_url'], 
                release_timestamp=video_info['release_timestamp'], 
                subtitles=video_info['subtitles'], 
                comment_count=video_info['comment_count'], 
                like_count=video_info['like_count'],  
                uploader=video_info['uploader'], 
                uploader_id=video_info['uploader_id'], 
                uploader_url=video_info['uploader_url'], 
                upload_date=video_info['upload_date'], 
                fulltitle=video_info['fulltitle'], 
                duration_string=video_info['duration_string'], 
                format=video_info['format'], 
                format_id=video_info['format_id'], 
                ext=video_info['ext'], 
                language=video_info['language'], 
                filesize_approx=video_info['filesize_approx'], 
                audio_channels=video_info['audio_channels'])
            # Write the result to a json file
            json.dump(result.dict(), open(metadata_file, 'w'))

            return result
        except youtube_dl.utils.DownloadError as e:
            logger.error("The video could not be downloaded")
            raise e

if __name__ == '__main__':
    video_url = 'https://www.youtube.com/watch?v=P4urfQ1BdGI'
    result = download_video_flow(video_url)
    # print(result)