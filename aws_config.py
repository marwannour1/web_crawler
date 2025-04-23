#!/usr/bin/env python3
# filepath: aws_config.py

"""
AWS configuration for the distributed web crawler.
Contains settings for SQS, DynamoDB, and S3.
"""

import os
import boto3
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS Credentials - load from environment variables
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "eu-north-1")

# SQS Configuration
SQS_CRAWLER_QUEUE_NAME = "webcrawler-crawler-queue"
SQS_INDEXER_QUEUE_NAME = "webcrawler-indexer-queue"

# DynamoDB Configuration
DYNAMODB_TABLE_NAME = "webcrawler-tasks"
DYNAMODB_RESULTS_TTL = 86400  # 24 hours in seconds

# S3 Configuration
S3_BUCKET_NAME = "webcrawler-content-marwan"
S3_PREFIX = "crawled-pages/"  # Prefix for organizing content in the bucket

# Connection clients
sqs_client = None
dynamodb_client = None
s3_client = None

def init_aws_clients():
    """Initialize AWS clients with credentials"""
    global sqs_client, dynamodb_client, s3_client

    try:
        # Create session with credentials
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        # Create clients
        sqs_client = session.client('sqs')
        dynamodb_client = session.client('dynamodb')
        s3_client = session.client('s3')

        logger.info("AWS clients initialized successfully")
        return True

    except ClientError as e:
        logger.error(f"Failed to initialize AWS clients: {e}")
        return False

def setup_aws_resources():
    """Create necessary AWS resources if they don't exist"""
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        logger.error("AWS credentials not found in environment variables")
        return False

    if not init_aws_clients():
        return False

    success = True

    # Create SQS queues
    try:
        # Create crawler queue
        crawler_queue = sqs_client.create_queue(
            QueueName=SQS_CRAWLER_QUEUE_NAME,
            Attributes={
                'VisibilityTimeout': '300',  # 5 minutes
                'MessageRetentionPeriod': '86400'  # 1 day
            }
        )
        logger.info(f"Crawler queue created/confirmed: {SQS_CRAWLER_QUEUE_NAME}")

        # Create indexer queue
        indexer_queue = sqs_client.create_queue(
            QueueName=SQS_INDEXER_QUEUE_NAME,
            Attributes={
                'VisibilityTimeout': '300',
                'MessageRetentionPeriod': '86400'
            }
        )
        logger.info(f"Indexer queue created/confirmed: {SQS_INDEXER_QUEUE_NAME}")

    except ClientError as e:
        logger.error(f"Error setting up SQS queues: {e}")
        success = False

    # Create DynamoDB table for results
    # Create DynamoDB table for results
    try:
        try:
            dynamodb_client.describe_table(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"DynamoDB table already exists: {DYNAMODB_TABLE_NAME}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table doesn't exist, create it
                dynamodb_client.create_table(
                    TableName=DYNAMODB_TABLE_NAME,
                    KeySchema=[
                        {'AttributeName': 'task_id', 'KeyType': 'HASH'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'task_id', 'AttributeType': 'S'}
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
                logger.info(f"DynamoDB table created: {DYNAMODB_TABLE_NAME}")

                # Wait for table to become available
                logger.info("Waiting for table to become active...")
                waiter = dynamodb_client.get_waiter('table_exists')
                waiter.wait(TableName=DYNAMODB_TABLE_NAME)

                # Now enable TTL as a separate operation
                try:
                    dynamodb_client.update_time_to_live(
                        TableName=DYNAMODB_TABLE_NAME,
                        TimeToLiveSpecification={
                            'Enabled': True,
                            'AttributeName': 'expires_at'
                        }
                    )
                    logger.info(f"TTL enabled for table: {DYNAMODB_TABLE_NAME}")
                except ClientError as ttl_error:
                    logger.warning(f"Could not enable TTL: {ttl_error}")
            else:
                raise
    except ClientError as e:
        logger.error(f"Error setting up DynamoDB table: {e}")
        success = False

    # Create S3 bucket
    try:
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            logger.info(f"S3 bucket already exists: {S3_BUCKET_NAME}")
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == '403':
                # Bucket doesn't exist or we don't have permission to check it
                try:
                    if AWS_REGION == 'us-east-1':
                        s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
                    else:
                        s3_client.create_bucket(
                            Bucket=S3_BUCKET_NAME,
                            CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                        )
                    logger.info(f"S3 bucket created: {S3_BUCKET_NAME}")
                except ClientError as create_error:
                    logger.warning(f"Could not create S3 bucket: {create_error}")
                    logger.warning("Continuing without S3 storage - will use filesystem instead")
                    # Don't mark success as False, just continue without S3
            else:
                raise
    except ClientError as e:
        logger.warning(f"Error setting up S3 bucket: {e}")
        logger.warning("Continuing without S3 storage - will use filesystem instead")
        # Don't fail setup completely just because S3 is unavailable

    return success


def ensure_aws_clients():
    """Ensure AWS clients are initialized before using them"""
    global sqs_client, dynamodb_client, s3_client
    if sqs_client is None or dynamodb_client is None or s3_client is None:
        return init_aws_clients()
    return True

def get_queue_url(queue_name):
    """Get the URL for a queue"""
    ensure_aws_clients()
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']
    except ClientError as e:
        logger.error(f"Error getting queue URL for {queue_name}: {e}")
        return None

# Queue URL getters
def get_crawler_queue_url():
    return get_queue_url(SQS_CRAWLER_QUEUE_NAME)

def get_indexer_queue_url():
    return get_queue_url(SQS_INDEXER_QUEUE_NAME)