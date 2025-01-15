import logging
import os
import subprocess
from datetime import datetime

import boto3

# S3 클라이언트 초기화
s3_client = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
destination = os.environ.get('Destination')


def lambda_handler(event, context):
    """
    S3에 저장된 비디오 파일들을 병합하여 최종 파일을 저장하는 Lambda 함수

    Args:
        event: 이전 단계에서 전달된 S3 파일 목록
        context: Lambda context object

    Returns:
        dict: 병합 결과 (최종 파일 경로)
    """

    # 이벤트에서 입력 데이터 추출
    original_file_name = event.get('originalFileName')
    bucket = event.get('bucket')
    file_name = original_file_name + '.mp4'
    encoded_parts = event.get('encodedParts', [])  # 병합할 파일 목록 (S3 경로 배열)
    output_key = destination + "/" + original_file_name + "/Default/MP4/" + file_name

    # 데이터가 없다면 진행 안함
    if len(encoded_parts) == 0:
        return {
            "status": "failed",
            "error_message": "no data"
        }

    # S3에 파일 존재 여부 확인
    incomplete_parts = encoded_parts.copy()
    now = datetime.now()
    s3_timeout = int(os.environ.get('S3Timeout', 300))  # S3 파일 존재 여부 확인 타임아웃 (초)
    while len(incomplete_parts) != 0:
        if (datetime.now() - now).seconds > s3_timeout:
            return {
                "status": "failed",
                "error_message": "incomplete parts"
            }
        for part in incomplete_parts:
            try:
                s3_client.head_object(Bucket=bucket, Key=part)
                incomplete_parts.remove(part)
            except Exception as e:
                # 파일이 존재하지 않거나 다른 오류가 발생한 경우
                # logger.error(f"Error checking file {part} in bucket {bucket}: {e}")
                pass

    # 로컬 경로 설정
    local_tmp_dir = "/tmp"
    local_list_file = os.path.join(local_tmp_dir, "file_list.txt")
    local_output_file = os.path.join(local_tmp_dir, "output.mp4")

    try:
        # 병합할 파일 다운로드 및 리스트 파일 생성
        with open(local_list_file, 'w') as list_file:
            for part_key in encoded_parts:
                local_file_path = os.path.join(local_tmp_dir, os.path.basename(part_key))
                logger.info(f"Downloading {part_key} from bucket {bucket} to {local_file_path}")
                s3_client.download_file(bucket, part_key, local_file_path)

                # ffmpeg 병합용 리스트 파일 작성
                list_file.write(f"file '{local_file_path}'\n")

        # ffmpeg를 사용하여 파일 병합
        ffmpeg_command = [
            "ffmpeg", "-f", "concat", "-safe", "0",
            "-i", local_list_file, "-c", "copy", local_output_file
        ]
        logger.info(f"Running ffmpeg command: {' '.join(ffmpeg_command)}")
        subprocess.run(ffmpeg_command, check=True)

        # 병합된 파일 S3 업로드
        logger.info(f"Uploading merged file to bucket {bucket} with key {output_key}")
        s3_client.upload_file(local_output_file, bucket, output_key)

        # 결과 반환
        return {
            "status": "success",
            "bucket": bucket,
            "output_file": f"s3://{bucket}/{output_key}"
        }

    except Exception as e:
        logger.error(f"Error during video merging: {e}")
        return {
            "status": "failed",
            "error_message": str(e)
        }

    finally:
        # 로컬 파일 정리
        remove_file(local_tmp_dir)


def remove_file(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            for file in os.listdir(path):
                remove_file(os.path.join(path, file))
            if path != "/tmp":  # /tmp 디렉토리는 유지
                os.rmdir(path)
