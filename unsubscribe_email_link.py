import os
import re
import json
import boto3
import logging
import watchtower
from boto3.dynamodb.conditions import Attr
from utils.aws_utils import AWSUtils

# Setup CloudWatch logging
logger = logging.getLogger("unsubscribe_email_logger")
logger.setLevel(logging.INFO)
cw_handler = watchtower.CloudWatchLogHandler(log_group="UnsubscribeEmailLogs")
logger.addHandler(cw_handler)

# Client to interact with DynamoDB and email records
class EmailDBClient:
    def __init__(self, table_name: str):
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)
        logger.info(f"Initialized EmailDBClient for table: {table_name}")

    # Paginate through records filtered by category
    def get_all_promotional_emails(self):
        items = []
        last_key = None
        logger.info("Starting scan for promotional emails.")

        while True:
            scan_params = {
                "FilterExpression": Attr("category").eq("promotions")
            }
            if last_key:
                scan_params["ExclusiveStartKey"] = last_key

            response = self.table.scan(**scan_params)
            batch = response.get("Items", [])
            logger.info(f"Fetched batch of {len(batch)} promotional emails.")
            items.extend(batch)

            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break

        logger.info(f"Total promotional emails retrieved: {len(items)}")
        return items

    # Extract unsubscribe links from HTML email bodies
    def extract_unsubscribe_links(self, emails):
        result = []
        logger.info(f"Extracting unsubscribe links from {len(emails)} emails.")
        for record in emails:
            sender = record.get("sender")
            html_body = record.get("body_html", "")
            links = re.findall(r'https?://[^\s">]*unsubscribe[^\s">]*', html_body)
            for link in links:
                logger.info(f"Found unsubscribe link for sender {sender}: {link}")
                result.append({
                    "sender": sender,
                    "unsubscribe_url": link
                })
        logger.info(f"Total unsubscribe links extracted: {len(result)}")
        return result

# Upload final JSON mapping to S3
def upload_to_s3(bucket: str, key: str, data: dict):
    s3 = boto3.client("s3")
    logger.info(f"Uploading result to S3 bucket: {bucket}, key: {key}")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )
    logger.info("Upload to S3 completed.")

def upload_error_to_s3(bucket: str, key: str, error_message: str):
    s3 = boto3.client("s3")
    logger.error(f"Uploading error file to S3 bucket: {bucket}, key: {key}")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps({"error": error_message}),
        ContentType="application/json"
    )
    logger.error("Error file upload to S3 completed.")

# Main workflow
if __name__ == "__main__":
    try:
        # Load config and initialize AWS helpers
        config_path = os.path.join(os.path.dirname(__file__), ".config")
        aws_config = AWSUtils(config_path)
        logger.info("Loaded AWS configuration.")

        # Retrieve secrets and parameters
        s3_bucket = aws_config.get_s3_bucket()
        output_key = aws_config.get_s3_upload_key()
        table_name = "EmailTable"  # Replace with your actual DynamoDB table name
        logger.info(f"Parameters: S3 bucket={s3_bucket}, output key={output_key}, table name={table_name}")

        # Query and process DynamoDB email records
        email_db = EmailDBClient(table_name)
        promo_emails = email_db.get_all_promotional_emails()
        unsubscribe_map = email_db.extract_unsubscribe_links(promo_emails)

        # Structure the results as a JSON table
        result_table = []
        for entry in unsubscribe_map:
            result_table.append({
                "sender": entry["sender"],
                "unsubscribe_url": entry["unsubscribe_url"]
            })

        # Upload the result to S3
        upload_to_s3(bucket=s3_bucket, key=output_key, data={"unsubscribe_table": result_table})
        logger.info("Script execution completed.")

    except Exception as e:
        logger.error(f"Failed to process emails: {e}")
        # Upload error file to S3
        try:
            config_path = os.path.join(os.path.dirname(__file__), ".config")
            aws_config = AWSUtils(config_path)
            s3_bucket = aws_config.get_s3_bucket()
            error_key = "unsuccesful_retreiveal.json"
            upload_error_to_s3(bucket=s3_bucket, key=error_key, error_message=str(e))
        except Exception as upload_err:
            logger.error(f"Failed to upload error file to S3: {upload_err}")