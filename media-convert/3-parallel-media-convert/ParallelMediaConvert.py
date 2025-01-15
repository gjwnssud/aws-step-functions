import json
import logging
import os
import subprocess
import uuid
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3 = boto3.resource('s3')

SIGNED_URL_EXPIRATION = 300  # The number of seconds that the Signed URL is valid


def handler(event, context):
    assetID = str(uuid.uuid4())
    sourceS3Bucket = event['bucket']
    sourceS3Key = event['part']
    sourceS3 = 's3://' + sourceS3Bucket + '/' + sourceS3Key
    mediaConvertRole = os.environ['MediaConvertRole']
    mediaConvertEndpoint = os.environ['MediaConvertEndpoint']
    application = os.environ['Application']
    region = os.environ['AWS_DEFAULT_REGION']
    jobs = []
    width = None
    gWidth = 960
    height = None
    gHeight = 540
    aBitrate = 0
    vBitrate = 0

    # Use MediaConvert SDK UserMetadata to tag jobs with the assetID
    # Events from MediaConvert will have the assetID in UserMedata
    jobMetadata = {}
    jobMetadata['assetID'] = assetID
    jobMetadata['application'] = application
    jobMetadata['input'] = sourceS3

    return_value = {
        'status': 'success',
        'data': {}
    }

    try:

        # Build a list of jobs to run against the input.  Use the settings files in WatchFolder/jobs
        # if any exist.  Otherwise, use the default job.

        jobInput = {}
        # Iterates through all the objects in jobs folder of the WatchFolder bucket, doing the pagination for you. Each obj
        # contains a jobSettings JSON
        bucket = S3.Bucket(sourceS3Bucket)

        # Generate a signed URL for the uploaded asset
        logger.info('sourceS3Bucket: %s', sourceS3Bucket)
        logger.info('sourceS3Key: %s', sourceS3Key)

        try:
            signed_url = get_signed_url(SIGNED_URL_EXPIRATION, sourceS3Bucket, sourceS3Key)
            logger.info('signed_url: %s', signed_url)

            # Launch MediaInfo
            # Pass the signed URL of the uploaded asset to MediaInfo as an input
            # MediaInfo will extract the technical metadata from the asset
            # The extracted metadata will be outputted in XML format and
            # stored in the variable xml_output

            json_output = subprocess.check_output(["./mediainfo", "--full", "--output=JSON", signed_url])
            json_object = json.loads(json_output)

            logger.info("json_object: {}".format(json_object))

            for track in json_object['media']['track']:
                if track['@type'] == 'Video':
                    width = int(track['Width'])
                    height = int(track['Height'])
                    vBitrate = int(track['BitRate'])
                if track['@type'] == 'Audio':
                    aBitrate = int(track['BitRate'])
        except:
            width = 960
            height = 540

        logger.info('vBitrate: %d, aBitrate: %d', vBitrate, aBitrate)
        logger.info('width: %d, height: %d', width, height)

        for obj in bucket.objects.filter(Prefix='jobs/'):
            if obj.key != "jobs/":
                jobInput = {}
                jobInput['filename'] = obj.key
                logger.info('jobInput: %s', jobInput['filename'])

                jobInput['settings'] = json.loads(obj.get()['Body'].read())
                logger.info(json.dumps(jobInput['settings']))

                jobs.append(jobInput)

        # Use Default job settings in the lambda zip file in the current working directory
        if not jobs:
            with open('job.json') as json_data:
                jobInput['filename'] = 'Default'
                logger.info('jobInput: %s', jobInput['filename'])

                jobInput['settings'] = json.load(json_data)
                logger.info(json.dumps(jobInput['settings']))

                jobs.append(jobInput)

        # add the account-specific endpoint to the client session
        logger.info(f"mediaConvertEndpoint == {mediaConvertEndpoint}")
        client = boto3.client('mediaconvert', region_name=region, endpoint_url=mediaConvertEndpoint, verify=True)

        for j in jobs:
            jobSettings = j['settings']
            jobFilename = j['filename']

            # Save the name of the settings file in the job userMetadata
            jobMetadata['settings'] = jobFilename

            # Update the job settings with the source video from the S3 event
            jobSettings['Inputs'][0]['FileInput'] = sourceS3

            # Update the job settings with the destination paths for converted videos.  We want to replace the
            # destination bucket of the output paths in the job settings, but keep the rest of the
            # path
            file_dirname = os.path.dirname(sourceS3Key)
            original_file_name = file_dirname[file_dirname.rfind("/") + 1: len(file_dirname)]
            file_name = os.path.splitext(os.path.basename(sourceS3Key))[0]
            encoded_file_dest_bucket = 's3://' + os.environ['EncodedFileDestinationBucket'] + '/' \
                                       + original_file_name + '/'

            for outputGroup in jobSettings['OutputGroups']:
                logger.info("outputGroup['OutputGroupSettings']['Type'] == %s", outputGroup['OutputGroupSettings']['Type'])
                logger.info("outputGroup['CustomName'] == %s", outputGroup['CustomName'])
                for output in outputGroup['Outputs']:
                    ratio = None
                    if width >= height:
                        if width > 960:
                            gWidth = 960
                            ratio = 960 / width
                            gHeight = get_even_value(int(height * ratio))
                        else:
                            gWidth = width
                            gHeight = height
                        output['VideoDescription']['Width'] = gWidth
                        output['VideoDescription']['Height'] = gHeight
                        logger.info("VideoDescription landscape Width == %d", output['VideoDescription']['Width'])
                    else:
                        if height > 540:
                            gHeight = 540
                            ratio = 540 / height
                            gWidth = get_even_value(int(width * ratio))
                        else:
                            gHeight = height
                        output['VideoDescription']['Width'] = gWidth
                        output['VideoDescription']['Height'] = gHeight
                        logger.info("VideoDescription Portrait Height == %d", output['VideoDescription']['Height'])

                if outputGroup['OutputGroupSettings']['Type'] == 'FILE_GROUP_SETTINGS':
                    logger.info("encoded_file_dest_bucket == %s", encoded_file_dest_bucket)
                    outputGroup['OutputGroupSettings']['FileGroupSettings']['Destination'] = encoded_file_dest_bucket
                else:
                    logger.error("Exception: Unknown Output Group Type %s", outputGroup['OutputGroupSettings']['Type'])
                    raise ValueError("Unknown Output Group Type")

            # 첫번째 파일일 경우 썸네일 추출 세팅 등록
            if int(file_name.split("_")[0].replace("part", "")) == 0:
                try:
                    setThumbnailSettings(jobSettings, gWidth, gHeight, original_file_name)
                except Exception as e:
                    logger.error(f'failed setThumbnailSettings, Exception: {e}')
                    pass

            logger.info(json.dumps(jobSettings))

            # Convert the video using AWS Elemental MediaConvert
            job = client.create_job(Role=mediaConvertRole, UserMetadata=jobMetadata, Settings=jobSettings)

            return_value['data']['encodedPart'] = urlparse(encoded_file_dest_bucket).path.lstrip('/') + file_name + ".mp4"
            return_value['data']['originalFileName'] = original_file_name
            return_value['data']['bucket'] = sourceS3Bucket

    except Exception as e:
        logger.error('Exception: %s', e)
        return_value['status'] = "failed"
        return_value['message'] = str(e)
        raise

    finally:
        return return_value


def get_signed_url(expires_in, bucket, obj):
    """
    Generate a signed URL
    :param expires_in:  URL Expiration time in seconds
    :param bucket:
    :param obj:         S3 Key name
    :return:            Signed URL
    """
    s3_cli = boto3.client("s3")
    presigned_url = s3_cli.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj},
                                                  ExpiresIn=expires_in)
    return presigned_url


def get_even_value(num):
    if num % 2 == 0:
        return num
    else:
        return num + 1


def setThumbnailSettings(jobSettings, gWidth, gHeight, original_file_name):
    thumbnailSettings = {
        "CustomName": "Thumbnails",
        "Name": "File Group",
        "Outputs": [
            {
                "ContainerSettings": {
                    "Container": "RAW"
                },
                "VideoDescription": {
                    "Width": gWidth,
                    "ScalingBehavior": "DEFAULT",
                    "Height": gHeight,
                    "TimecodeInsertion": "DISABLED",
                    "AntiAlias": "ENABLED",
                    "Sharpness": 50,
                    "CodecSettings": {
                        "Codec": "FRAME_CAPTURE",
                        "FrameCaptureSettings": {
                            "FramerateNumerator": 1,
                            "FramerateDenominator": 1,
                            "MaxCaptures": 2,
                            "Quality": 80
                        }
                    },
                    "AfdSignaling": "NONE",
                    "DropFrameTimecode": "ENABLED",
                    "RespondToAfd": "NONE",
                    "ColorMetadata": "INSERT"
                }
            }
        ],
        "OutputGroupSettings": {
            "Type": "FILE_GROUP_SETTINGS",
            "FileGroupSettings": {
                "Destination": f"s3://{os.environ['DestinationBucket']}/{original_file_name}/Default/Thumbnails/",
                "DestinationSettings": {
                    "S3Settings": {
                        "AccessControl": {
                            "CannedAcl": "BUCKET_OWNER_FULL_CONTROL"
                        }
                    }
                }
            }
        }
    }
    jobSettings['OutputGroups'].append(thumbnailSettings)
