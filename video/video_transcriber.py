from prefect import flow, task, get_run_logger, variables
from prefect.tasks import task_input_hash
from google.cloud.speech_v2 import SpeechClient
from google.cloud.speech_v2.types import cloud_speech
from google.api_core.client_options import ClientOptions
from urllib.parse import urlparse

from video_storage import s3_download_file, gcs_upload_file

    
@flow
def transcribe_video(video_file_path: str) -> str:
    logger = get_run_logger()
    logger.info(f'Transcribing video {video_file_path}...')
    local_file = get_local_file(video_file_path)
    gcs_bucket = variables.get('gcs_bucket')
    gcs_folder = variables.get('gcs_folder')
    gcs_file = gcs_upload_file(local_file_path=local_file, gcs_bucket=gcs_bucket, gcs_folder=gcs_folder)
    # transcription = transcribe_batch_dynamic_batching_v2('speech-to-text-395212', gcs_file)
    return "transcription"

@task
def get_local_file(video_file_path: str) -> str:
    local_working_dir = variables.get('local_working_dir')
    if video_file_path.startswith('s3://'):
        # download from s3
        urlparse_result = urlparse(video_file_path)
        bucket = urlparse_result.netloc
        file_path = urlparse_result.path[1:]
        local_file = s3_download_file(s3_bucket=bucket, s3_file_path=file_path, local_directory=local_working_dir)
    else:
        # local file
        local_file = video_file_path
    return local_file


def transcribe_batch_dynamic_batching_v2(
    project_id: str,
    gcs_uri: str,
) -> cloud_speech.BatchRecognizeResults:
    """Transcribes audio from a Google Cloud Storage URI.

    Args:
        project_id: The Google Cloud project ID.
        gcs_uri: The Google Cloud Storage URI.

    Returns:
        The RecognizeResponse.
    """
    # Instantiates a client
    client = SpeechClient(
            client_options=ClientOptions(
            api_endpoint="europe-west4-speech.googleapis.com",
        )
    )

    config = cloud_speech.RecognitionConfig(
        auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
        language_codes=["iw-IL"],
        model="chirp",
    )

    file_metadata = cloud_speech.BatchRecognizeFileMetadata(uri=gcs_uri)

    request = cloud_speech.BatchRecognizeRequest(
        # recognizer=f"projects/{project_id}/locations/global/recognizers/_",
        recognizer=f"projects/{project_id}/locations/europe-west4/recognizers/_",
        config=config,
        files=[file_metadata],
        recognition_output_config=cloud_speech.RecognitionOutputConfig(
            inline_response_config=cloud_speech.InlineOutputConfig(),
        ),
        processing_strategy=cloud_speech.BatchRecognizeRequest.ProcessingStrategy.DYNAMIC_BATCHING,
    )

    # Transcribes the audio into text
    operation = client.batch_recognize(request=request)

    print("Waiting for operation to complete...")
    response = operation.result(timeout=120)

    for result in response.results[gcs_uri].transcript.results:
        print(f"Transcript: {result.alternatives[0].transcript}")

    return response.results[gcs_uri].transcript

# import time
# start = time.time()

# transcription = transcribe_batch_dynamic_batching_v2('speech-to-text-395212', 'gs://eu-west4-rantav-speech-to-text/audio-files/audio_compressed_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3')
# end = time.time()
# print(transcription)

# print(f'Execution time: {end - start}')

if __name__ == '__main__':
    transcribe_video(video_file_path='/Users/rantav/Downloads/tmp-whisper/audio_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3')
    # transcribe_video(video_file_path='s3://data-pipeline-video-downloads/local-test/video_downloads/audio_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3')
