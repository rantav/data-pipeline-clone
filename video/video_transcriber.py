import os
import tempfile
from urllib.parse import urlparse
import json

from prefect import flow, task, get_run_logger, variables
from prefect.tasks import task_input_hash
from google.cloud.speech_v2 import SpeechClient
from google.cloud.speech_v2.types import cloud_speech, RecognitionFeatures, SpeakerDiarizationConfig
from google.api_core.client_options import ClientOptions

from prefect_gcp import GcpCredentials

from video_storage import s3_download_file, gcs_upload_file, gcs_download_file
from video_transcoder import transcode_to_format

cahce_disabled = True


class TranscriptionResults:
    def __init__(self, gcs_video_files, text, language_code, raw_transcriptions: list[dict], max_duration_sec: int):
        self.gcs_video_files = gcs_video_files
        self.text = text
        self.language_code = language_code
        self.raw_transcriptions = raw_transcriptions
        self.max_duration_sec = max_duration_sec

    gcs_video_files: list[str]
    text: str
    language_code: str
    raw_transcriptions: list[dict]
    max_duration_sec: int


@flow
def transcribe_video(video_file_path: str, max_duration_sec: int = 3600) -> TranscriptionResults:
    """
    Transcribes a video (or audio) file to text.
    If needed, breaks down the file into smaller chunks and transcribes each chunk separately.
    Returns a transcription result object which may contain multiple transcriptions
    (one per chunk), after being joined together with markers.

    @param video_file_path: The video file path to transcribe. It can be a local file path or an s3:// path.
    @param max_duration_sec: The maximum duration of each transcoded file in seconds.

    @return: A TranscriptionResults object
    """
    logger = get_run_logger()
    logger.info(f"Transcribing video {video_file_path}...")
    local_file = get_local_file(video_file_path)
    local_mp3_files = transcode_to_format(local_file_path=local_file, format="mp3", max_duration_sec=max_duration_sec)

    gcs_base = variables.get("gcs_base")
    gcs_files = []
    for local_mp3_file in local_mp3_files:
        gcs_file = gcs_upload_file.submit(local_file_path=local_mp3_file, gcs_base=gcs_base)
        gcs_files.append(gcs_file)
    gcs_files = [gcs_file.result() for gcs_file in gcs_files]
    transcription_results = transcribe_chirp(gcs_files=gcs_files, max_duration_sec=max_duration_sec)
    return transcription_results


@task
def get_local_file(video_file_path: str) -> str:
    local_working_dir = variables.get("local_working_dir")
    if video_file_path.startswith("s3://"):
        # download from s3
        urlparse_result = urlparse(video_file_path)
        bucket = urlparse_result.netloc
        file_path = urlparse_result.path[1:]
        local_file = s3_download_file(s3_bucket=bucket, s3_file_path=file_path, local_directory=local_working_dir)
    else:
        # local file
        local_file = video_file_path
    return local_file


@task(cache_key_fn=task_input_hash, refresh_cache=cahce_disabled)
def transcribe_chirp(
    gcs_files: list[str],
    max_duration_sec: int = 3600,
    region: str = "europe-west4",
    api_endpoint: str = "europe-west4-speech.googleapis.com",
    transcription_timeout_sec: int = 3600,
) -> TranscriptionResults:
    """
    Transcribes a list of video (or audio) file to text.
    Collects all the results into a single transcription result.

    """
    logger = get_run_logger()

    creds = GcpCredentials.load("gcp-credentials")
    credentials_dict = creds.service_account_info.get_secret_value()
    project_id = creds.project
    gcs_base = variables.get("gcs_base")
    parsed = urlparse(gcs_base)
    bucket = parsed.netloc
    folder = parsed.path[1:]

    # Instantiates a client
    client = SpeechClient.from_service_account_info(
        credentials_dict, client_options=ClientOptions(api_endpoint=api_endpoint)
    )
    language_code = "iw-IL"
    config = cloud_speech.RecognitionConfig(
        auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
        language_codes=[language_code],
        model="chirp",
        features=RecognitionFeatures(
            # Add timestamp per word. Not sure if it's really needed
            # enable_word_time_offsets=True,
            # Speaker diarization (who said what)
            # diarization_config=SpeakerDiarizationConfig(min_speaker_count=1, max_speaker_count=6), #Not supported by chirp?
        ),
    )

    files_metadata = [cloud_speech.BatchRecognizeFileMetadata(uri=gcs_uri) for gcs_uri in gcs_files]
    request = cloud_speech.BatchRecognizeRequest(
        recognizer=f"projects/{project_id}/locations/{region}/recognizers/_",
        config=config,
        files=files_metadata,
        recognition_output_config=cloud_speech.RecognitionOutputConfig(
            gcs_output_config=cloud_speech.GcsOutputConfig(uri=f"gs://{bucket}/{folder}/transcriptions/"),
        ),
        processing_strategy=cloud_speech.BatchRecognizeRequest.ProcessingStrategy.DYNAMIC_BATCHING,
    )

    # Transcribes the audio into text
    operation = client.batch_recognize(request=request)

    logger.info(f"Waiting for transcription operation to complete for {gcs_files}...")
    response = operation.result(timeout=transcription_timeout_sec)
    results = download_gcs_transcription_results(response)
    joined_transcripts = []
    raw_transcriptions = []
    file_number = 0
    for gcs_uri in gcs_files:
        joined_transcript = ". ".join([r["alternatives"][0]["transcript"] for r in results[gcs_uri]["results"]])
        logger.debug(f"Finished transcription for {gcs_uri}, result: {len(joined_transcript)} chars.")
        offset = file_number * max_duration_sec
        joined_transcripts.append(f" ||| File number {file_number}, offset {offset} sec (source={gcs_uri}) ||| ")
        joined_transcripts.append(joined_transcript)
        raw_transcriptions.append(results[gcs_uri])
        file_number += 1

    joined_transcripts = " ".join(joined_transcripts)

    ret = TranscriptionResults(
        gcs_video_files=gcs_files,
        text=joined_transcripts,
        language_code=language_code,
        raw_transcriptions=raw_transcriptions,
        max_duration_sec=max_duration_sec,
    )
    return ret


def download_gcs_transcription_results(response: cloud_speech.BatchRecognizeResponse) -> dict[str, str]:
    logger = get_run_logger()
    results = {}
    tmp_dir = tempfile.mkdtemp()

    for query_path, batch_result in response.results.items():
        local_result = gcs_download_file(gcs_uri=batch_result.uri, local_directory=tmp_dir)
        with open(local_result, "r") as lr:
            result = json.load(lr)
        results[query_path] = result
    return results


if __name__ == "__main__":
    # res = transcribe_video(video_file_path='/Users/rantav/Downloads/tmp-whisper/audio_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3', max_duration_sec=300)
    res = transcribe_video(
        video_file_path="s3://data-pipeline-video-downloads/local-test/video_downloads/audio_UCKEImtWikw9usC1pl_9m1nQ_20230705_Hoy7fV6IQ8Y.mp3"
    )

    print(res.text)
