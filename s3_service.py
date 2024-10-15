import os
import boto3
from botocore.exceptions import NoCredentialsError, BotoCoreError
from logger import logger
from config import settings

s3 = boto3.client(
    "s3",
    aws_access_key_id=settings.AWS_S3_ACCESS_KEY,
    aws_secret_access_key=settings.AWS_S3_ACCESS_SECRET,
    region_name=settings.AWS_S3_REGION,
)


def upload_to_s3(file_name):
    try:
        # Check if file exists in csv folder
        file_path = os.path.join("csv", file_name)
        if os.path.exists(file_path):
            # Upload file to S3
            s3.upload_file(file_path, settings.AWS_S3_BUCKET, settings.AWS_S3_key)
            logger.info("Successfully uploaded file to S3")
        else:
            logger.error("File with name=%s does not exist", file_name)
    except NoCredentialsError as cred_error:
        logger.error("No S3 credentials provided: %s", cred_error)
    except BotoCoreError as boto_error:
        logger.error("S3 error occurred: %s", boto_error)
    except Exception as e:
        logger.error("Error occurred while attempting to upload to S3: %s", e)
        raise e
