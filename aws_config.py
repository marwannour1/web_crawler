#!/usr/bin/env python3
# filepath: aws_config.py

"""
AWS configuration for the distributed web crawler.
Contains settings for SQS, DynamoDB, S3, and OpenSearch.
Handles creation of resources and utility functions.
"""

import os
import boto3
import time
import logging
import json
import requests
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth

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
S3_INPUT_PREFIX = "input/"  # Prefix for input content
S3_OUTPUT_PREFIX = "output/"  # Prefix for output content
S3_CONFIG_PREFIX = "config/"  # Prefix for configuration

# Connection clients
sqs_client = None
dynamodb_client = None
s3_client = None

def init_aws_clients():
    """Initialize AWS clients with credentials"""
    global sqs_client, dynamodb_client, s3_client

    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        logger.error("AWS credentials not found in environment variables")
        return False

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

def fix_dynamodb_table():
    """Fix the DynamoDB table schema to work with Celery"""
    try:
        ensure_aws_clients()

        # Check if table exists
        try:
            dynamodb_client.describe_table(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"Table {DYNAMODB_TABLE_NAME} exists, checking schema...")

            # We're simply going to recreate the table to ensure correct schema
            dynamodb_client.delete_table(TableName=DYNAMODB_TABLE_NAME)

            # Wait for the table to be deleted
            waiter = dynamodb_client.get_waiter('table_not_exists')
            waiter.wait(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"Table {DYNAMODB_TABLE_NAME} deleted successfully")

        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                logger.error(f"Error checking table: {e}")
                return False

        # Create the table with the correct schema for Celery
        dynamodb_client.create_table(
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},  # Use 'id' instead of 'task_id'
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        # Wait for the table to be created
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=DYNAMODB_TABLE_NAME)

        # Enable TTL on the new table
        dynamodb_client.update_time_to_live(
            TableName=DYNAMODB_TABLE_NAME,
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': 'expires'  # Use 'expires' instead of 'expires_at'
            }
        )

        logger.info(f"Table {DYNAMODB_TABLE_NAME} created successfully with correct schema")
        return True

    except Exception as e:
        logger.error(f"Error fixing DynamoDB table: {e}")
        return False

def test_opensearch_connection():
    """Test and determine the best OpenSearch authentication method"""
    try:
        # Load OpenSearch details from environment
        from distributed_config import OPENSEARCH_ENDPOINT, OPENSEARCH_USER, OPENSEARCH_PASS

        if not OPENSEARCH_ENDPOINT:
            logger.warning("OpenSearch endpoint not defined in environment")
            return False, None

        # Try AWS4Auth first (best for AWS OpenSearch)
        auth = AWS4Auth(
            AWS_ACCESS_KEY,
            AWS_SECRET_KEY,
            AWS_REGION,
            'es'  # service name for OpenSearch
        )

        # Test connection with AWS4Auth
        response = requests.get(
            f"{OPENSEARCH_ENDPOINT}/_cluster/health",
            auth=auth,
            headers={"Content-Type": "application/json"},
            timeout=10,
            verify=True
        )

        if response.status_code == 200:
            logger.info("AWS4Auth connection to OpenSearch successful!")
            logger.info(f"Cluster health: {response.json()}")

            # Save auth method for future use
            with open("opensearch_auth_method.txt", "w") as f:
                f.write("aws4auth")

            return True, "aws4auth"
        else:
            logger.warning(f"AWS4Auth failed with status code {response.status_code}")

        # Try basic auth
        response = requests.get(
            f"{OPENSEARCH_ENDPOINT}/_cluster/health",
            auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
            headers={"Content-Type": "application/json"},
            timeout=10,
            verify=True
        )

        if response.status_code == 200:
            logger.info("Basic auth connection to OpenSearch successful!")
            logger.info(f"Cluster health: {response.json()}")

            # Save auth method for future use
            with open("opensearch_auth_method.txt", "w") as f:
                f.write("basic")

            return True, "basic"
        else:
            logger.error(f"Basic auth failed with status code {response.status_code}")

        logger.error("All authentication methods failed for OpenSearch")
        return False, None

    except Exception as e:
        logger.error(f"Error testing OpenSearch connection: {e}")
        return False, None

def setup_aws_resources():
    """Create necessary AWS resources if they don't exist"""
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

    # Setup DynamoDB table with the fixed schema
    if not fix_dynamodb_table():
        success = False

    # Create S3 bucket and directory structure
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
                    logger.error(f"Could not create S3 bucket: {create_error}")
                    logger.error("S3 storage is required for this application")
                    success = False
            else:
                raise

        # Create S3 directory structure
        if success:
            create_s3_directories()

    except ClientError as e:
        logger.error(f"Error setting up S3 bucket: {e}")
        success = False

    # Test OpenSearch connection
    try:
        test_opensearch_connection()
    except Exception as e:
        logger.warning(f"OpenSearch connection test failed: {e}")
        logger.warning("Search functionality may be limited")

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

# Create S3 directory structure
def create_s3_directories():
    """Create S3 directory structure for input, output, and config"""
    ensure_aws_clients()
    try:
        # Create input directory
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_INPUT_PREFIX,
            Body=''
        )

        # Create output directory
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_OUTPUT_PREFIX,
            Body=''
        )

        # Create config directory
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_CONFIG_PREFIX,
            Body=''
        )

        logger.info(f"Created S3 directory structure in {S3_BUCKET_NAME}")
        return True
    except Exception as e:
        logger.error(f"Error creating S3 directories: {e}")
        return False

# Helper function to store configuration in S3
def store_config_in_s3(config):
    """Store crawler configuration in S3"""
    ensure_aws_clients()
    try:
        key = f"{S3_CONFIG_PREFIX}crawler_config.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps(config, indent=2).encode(),
            ContentType='application/json'
        )
        logger.info(f"Configuration saved to S3: s3://{S3_BUCKET_NAME}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error saving configuration to S3: {e}")
        return False

# Helper function to retrieve configuration from S3
def get_config_from_s3():
    """Retrieve crawler configuration from S3"""
    ensure_aws_clients()
    try:
        key = f"{S3_CONFIG_PREFIX}crawler_config.json"
        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key=key
        )
        config = json.loads(response['Body'].read().decode())
        logger.info(f"Configuration retrieved from S3: s3://{S3_BUCKET_NAME}/{key}")
        return config
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.warning(f"No configuration found in S3, using default")
            return None
        else:
            logger.error(f"Error retrieving configuration from S3: {e}")
            return None
    except Exception as e:
        logger.error(f"Error retrieving configuration from S3: {e}")
        return None