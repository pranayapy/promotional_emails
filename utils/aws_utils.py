import boto3
import configparser
import os
import logging

logger = logging.getLogger("aws_utils_logger")

# Utility class to fetch config-driven secrets and values from SSM
class AWSUtils:
    def __init__(self, config_path: str):
        self.config = configparser.ConfigParser()
        try:
            self.config.read(config_path)
            logger.info(f"Loaded config from {config_path}")
        except Exception as e:
            logger.error(f"Failed to read config file: {e}")
            raise
        self.ssm = boto3.client("ssm")

    def get_ssm_parameter(self, key: str, with_decryption=True) -> str:
        try:
            response = self.ssm.get_parameter(Name=key, WithDecryption=with_decryption)
            logger.info(f"Retrieved SSM parameter: {key}")
            return response["Parameter"]["Value"]
        except Exception as e:
            logger.error(f"Failed to retrieve SSM parameter '{key}': {e}")
            raise
    #TODO
    def get_gmail_credentials(self):
        try:
            email_key = self.config.get("gmail", "email")
            password_key = self.config.get("gmail", "password")
            email = self.get_ssm_parameter(email_key)
            password = self.get_ssm_parameter(password_key)
            logger.info("Fetched Gmail credentials from SSM.")
            return email, password
        except Exception as e:
            logger.error(f"Failed to get Gmail credentials: {e}")
            raise

    def get_s3_bucket(self):
        try:
            s3_upload_path = self.config.get("s3", "bucket")
            bucket = self.get_ssm_parameter(s3_upload_path, with_decryption=False)
            logger.info(f"Fetched S3 bucket name from SSM: {bucket}")
            return bucket
        except Exception as e:
            logger.error(f"Failed to get S3 bucket: {e}")
            raise

    def get_s3_upload_key(self):
        try:
            key = self.config.get("s3", "upload_key")
            logger.info(f"Fetched S3 upload key from config: {key}")
            return key
        except Exception as e:
            logger.error(f"Failed to get S3 upload key: {e}")
            raise