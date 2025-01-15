import boto3
import logging
import os
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

stepfunctions_client = boto3.client('stepfunctions')

def lambda_handler(event, context):
    """
    S3 트리거를 통해 실행되는 Lambda 함수.
    Step Functions 워크플로우를 시작합니다.
    """
    try:
        # S3 이벤트 정보 가져오기
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        # Step Functions 실행 입력 데이터 생성
        input_data = {
            "bucket": bucket_name,
            "key": object_key
        }

        # Step Functions ARN
        state_machine_arn = os.environ['STATE_MACHINE_ARN']

        # Step Functions 실행
        response = stepfunctions_client.start_execution(
                stateMachineArn=state_machine_arn,
                input=json.dumps(input_data)
        )

        logger.info(f"Started Step Functions execution: {response['executionArn']}")
        return {"status": "success", "executionArn": response['executionArn']}

    except Exception as e:
        logger.error(f"Error starting Step Functions: {str(e)}")
        return {"status": "failed", "error": str(e)}
