import os
import subprocess

from prefect import flow, task, get_run_logger, variables
from prefect.tasks import task_input_hash

cahce_disabled = False

@flow
def transcode_to_format(local_file_path: str, format: str, max_duration_sec: int = 3600) -> list[str]:
    '''
    Transcodes a local file to the specified format.

    @param local_file_path: The local file path to transcode
    @param format: The format to transcode to (e.g. mp3)
    @param max_duration_sec: The maximum duration of each transcoded file in seconds. 
        If the file is too long (e.g. couple of hours) 
        then the transcoded files will be split into multiple files of this duration.

    @return: A list of transcoded file paths, no more than max_duration_sec duration each
    '''
    logger = get_run_logger()
    logger.info(f'Transcoding {local_file_path} to {format}...')
    
    if not os.path.exists(local_file_path):
        raise ValueError(f'File {local_file_path} does not exist')

    transcoded_files = transcode_ffmpeg(local_file_path, format, max_duration_sec=max_duration_sec)
    
    return transcoded_files

@task(cache_key_fn=task_input_hash, refresh_cache=cahce_disabled)
def transcode_ffmpeg(local_file_path: str, format: str, max_duration_sec: int = 3600) -> list[str]:
    logger = get_run_logger()
    if is_file_extension(local_file_path, format):
        logger.info(f'File {local_file_path} is already {format}, continuing anyway, in case it needs to be split')

    output_dir = os.path.dirname(local_file_path)
    file_name_without_extension = os.path.splitext(os.path.basename(local_file_path))[0]
    outout_file_template = os.path.join(output_dir, f'{file_name_without_extension}_segment%03d.{format}')
    logger.info(f'Transcoding {local_file_path} to {format} with max duration {max_duration_sec} seconds, output file template {outout_file_template}...')
    # Run ffmpeg
    if format == 'mp3':    
        cmd = [
            'ffmpeg',
            '-i', local_file_path,
            '-y',                                       # Force overwrite
            '-vn',                                      # Disable video processing
            '-c:a', 'libmp3lame',                       # Use MP3 codec
            '-q:a', '2',                                # Audio quality level (0-9, 2 recommended)
            '-f', 'segment',                            # Output format is segment
            '-segment_time', str(max_duration_sec),     # Segment duration in seconds
            outout_file_template                        # Output file template
        ]
    else:
        raise ValueError(f'Format {format} not supported')

    # First, delete all files that match the output file template
    for filename in os.listdir(output_dir):
        if filename.startswith(f'{file_name_without_extension}_segment') and filename.endswith(f'.{format}'):
            os.remove(os.path.join(output_dir, filename))

    # Run ffmpeg
    try:
        completed_process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if completed_process.returncode != 0:
            raise subprocess.CalledProcessError(completed_process.returncode, cmd, output=completed_process.stdout, stderr=completed_process.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(f'FFmpeg command failed with the following error: {e.stderr}')
        raise e

    # Collect output files
    output_files = []
    for filename in sorted(os.listdir(output_dir)):
        if filename.startswith(f'{file_name_without_extension}_segment') and filename.endswith(f'.{format}'):
            output_files.append(os.path.join(output_dir, filename))


    return output_files

def is_file_extension(file_path, extension):
    _, ext = os.path.splitext(file_path)
    return ext.lower() == f'.{extension.lower()}'

if __name__ == "__main__":
    # transcode_to_format(local_file_path='video_downloads/UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp4', format='mp3')
    transcode_to_format(local_file_path='video_downloads/UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp4', format='mp3', max_duration_sec=300)
    # transcode_to_format(local_file_path='/Users/rantav/Downloads/FULL-2023-04-21--00-29-38--to--2023-04-21--18-30-02.mkv', format='mp3')
    
