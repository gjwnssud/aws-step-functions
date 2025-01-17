import logging
import os
import subprocess

import boto3

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    bucket_name = event['bucket']
    object_key = event['key']
    segment_duration = os.environ["SegmentDuration"]
    inpu_contents_path = os.environ["InputContentsPath"]

    # 로컬 경로 설정
    path = os.path.dirname(object_key)
    file = os.path.basename(object_key)
    filename = os.path.splitext(file)[0]
    local_root = "/tmp"
    local_input_file = f"{local_root}/{file}"
    local_parts_output = f"{local_root}/{filename}"
    os.makedirs(local_parts_output, exist_ok=True)
    local_parts_output_pattern = f"{local_parts_output}/part%03d_{file}"

    # S3에서 입력 파일 다운로드
    logger.info(f"Downloading {object_key} from bucket {bucket_name}")
    s3.download_file(bucket_name, object_key, local_input_file)

    try:
        # 1. 파일 용량 확인
        file_size_limit = int(os.environ["FileSizeLimit"])
        file_size = os.path.getsize(local_input_file)
        if file_size > file_size_limit:  # 초과
            logger.warning("File size exceeds 3GB. Exiting function.")
            return {"status": "failed", "reason": "File size exceeds 3GB"}

        file_split_size_limit = int(os.environ["FileSplitSizeLimit"])
        if file_size <= file_split_size_limit:
            # 분할 없이 원본 반환
            logger.info(f"File size is less than {file_split_size_limit / (1024 * 1024):.2f}MB. Proceeding with direct upload.")
            return {
                "status": "success",
                "bucket": bucket_name,
                "parts": [object_key]
            }

        # 2. 영상 길이 확인
        logger.info("Checking video duration...")
        ffprobe_cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", local_input_file
        ]
        result = subprocess.run(ffprobe_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        video_duration = float(result.stdout.strip())

        part_files = []
        if video_duration < int(segment_duration):  # segment_duration(초) 미만
            # 분할 없이 원본 반환
            return {
                "status": "success",
                "bucket": bucket_name,
                "parts": [object_key]
            }

        logger.info(f"Video duration is {video_duration} seconds. Starting splitting process...")

        # 영상 길이가 segment_duration(초) 이상일 경우 분할 작업 수행
        command = [
            "ffmpeg", "-i", local_input_file,
            "-c", "copy", "-map", "0",
            "-segment_time", str(segment_duration),
            "-f", "segment", "-reset_timestamps", "1", local_parts_output_pattern
        ]
        subprocess.run(command, check=True)
        logger.info("Video splitting completed.")

        # 분할된 파일 업로드 및 목록 생성
        for part_file in os.listdir(local_parts_output):
            # S3에 업로드
            output_key = f"{path.replace(inpu_contents_path, inpu_contents_path + "_parts")}/{filename}/{part_file}"
            logger.info(f"Uploading {local_parts_output}/{part_file} to bucket {bucket_name} with key {output_key}")
            s3.upload_file(local_parts_output + "/" + part_file, bucket_name, output_key)
            part_files.append(output_key)

        # 처리 결과 반환
        return_value = {"status": "success", "bucket": bucket_name, "parts": part_files}
        logger.info(return_value)
        return return_value

    except subprocess.CalledProcessError as e:
        logger.error(f"Error during FFmpeg execution: {e}")
        return {"status": "failed", "reason": str(e)}

    finally:
        # 로컬 파일 정리
        remove_file(local_root)


def remove_file(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            for file in os.listdir(path):
                remove_file(os.path.join(path, file))
            if path != "/tmp":  # /tmp 디렉토리는 유지
                os.rmdir(path)
