from urllib.parse import urlparse

from prefect import flow, task, get_run_logger, variables
from prefect.tasks import task_input_hash
from google.cloud.speech_v2 import SpeechClient
from google.cloud.speech_v2.types import cloud_speech, RecognitionFeatures, SpeakerDiarizationConfig
from google.api_core.client_options import ClientOptions

from pydantic import BaseModel, ConfigDict

from prefect_gcp import GcpCredentials

from video_storage import s3_download_file, gcs_upload_file

class TranscriptionResult:
    def __init__(self, gcs_video_file, text, language_code, raw_transcription):
        self.gcs_video_file = gcs_video_file
        self.text = text
        self.language_code = language_code
        self.raw_transcription = raw_transcription
        
    gcs_video_file: str
    text: str
    language_code: str
    raw_transcription: cloud_speech.BatchRecognizeFileResult
    
@flow
def transcribe_video(video_file_path: str) -> TranscriptionResult:
    logger = get_run_logger()
    logger.info(f'Transcribing video {video_file_path}...')
    local_file = get_local_file(video_file_path)
    gcs_bucket = variables.get('gcs_bucket')
    gcs_folder = variables.get('gcs_folder')
    gcs_file = gcs_upload_file(local_file_path=local_file, gcs_bucket=gcs_bucket, gcs_folder=gcs_folder)
    transcription_result = transcribe_chirp(gcs_uri=gcs_file)
    return transcription_result

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

@task(cache_key_fn=task_input_hash)
def transcribe_chirp(
    gcs_uri: str,
    region: str = 'europe-west4',
    api_endpoint: str = 'europe-west4-speech.googleapis.com'
) -> TranscriptionResult:
    """Transcribes audio from a Google Cloud Storage URI.
    """
    logger = get_run_logger()

    creds = GcpCredentials.load("gcp-credentials")
    credentials_dict = creds.service_account_info.get_secret_value()
    project_id = creds.project

    # Instantiates a client
    client = SpeechClient.from_service_account_info(
        credentials_dict,
        client_options=ClientOptions(api_endpoint=api_endpoint)
    )
    language_code = 'iw-IL'
    config = cloud_speech.RecognitionConfig(
        auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
        language_codes=[language_code],
        model="chirp",
        features=RecognitionFeatures(
            # Add timestamp per word. Not sure if it's really needed
            # enable_word_time_offsets=True,

            # Speaker diarization (who said what)
            # diarization_config=SpeakerDiarizationConfig(min_speaker_count=1, max_speaker_count=6), Not supported by chirp?
        )
    )

    file_metadata = cloud_speech.BatchRecognizeFileMetadata(uri=gcs_uri)

    request = cloud_speech.BatchRecognizeRequest(
        recognizer=f"projects/{project_id}/locations/{region}/recognizers/_",
        config=config,
        files=[file_metadata],
        recognition_output_config=cloud_speech.RecognitionOutputConfig(
            inline_response_config=cloud_speech.InlineOutputConfig(),
        ),
        processing_strategy=cloud_speech.BatchRecognizeRequest.ProcessingStrategy.DYNAMIC_BATCHING,
    )

    # Transcribes the audio into text
    operation = client.batch_recognize(request=request)

    logger.info(f'Waiting for transcription operation to complete for {gcs_uri}...')
    response = operation.result(timeout=120)

    joined_transcript = '. '.join([r.alternatives[0].transcript for r in response.results[gcs_uri].transcript.results])
    logger.info(f'Finished transcription for {gcs_uri}, result: {len(joined_transcript)} chars.')

    ret = TranscriptionResult(
        gcs_video_file=gcs_uri,
        text=joined_transcript,
        language_code=language_code,
        raw_transcription=response.results[gcs_uri])
    return ret

if __name__ == '__main__':
    transcribe_video(video_file_path='/Users/rantav/Downloads/tmp-whisper/audio_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3')
    # transcribe_video(video_file_path='s3://data-pipeline-video-downloads/local-test/video_downloads/audio_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3')
