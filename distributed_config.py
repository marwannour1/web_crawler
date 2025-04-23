#!/usr/bin/env python3
# filepath: distributed_config.py

"""
Configuration for distributed web crawler deployment on AWS
Using AWS services (SQS, DynamoDB, S3) instead of Redis
"""
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Node IP addresses - still useful for health checks
MASTER_IP = "172.31.21.220"
CRAWLER_IP = "172.31.23.169"
INDEXER_IP = "172.31.20.112"

# AWS OpenSearch Service configuration
OPENSEARCH_ENDPOINT = os.environ.get("OPENSEARCH_ENDPOINT", "")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER", "elastic")
OPENSEARCH_PASS = os.environ.get("OPENSEARCH_PASS", "")

# AWS credentials (should be set in environment variables)
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "eu-north-1")

# Use AWS OpenSearch if endpoint is provided
if OPENSEARCH_ENDPOINT:
    ELASTICSEARCH_URL = OPENSEARCH_ENDPOINT
else:
    ELASTICSEARCH_URL = f"http://{INDEXER_IP}:9200"

# Node type - set appropriately on each instance
NODE_TYPE = os.environ.get("NODE_TYPE", "master")  # Change to "crawler" or "indexer" on respective nodes

# AWS SQS and DynamoDB configuration
from aws_config import (
    SQS_CRAWLER_QUEUE_NAME,
    SQS_INDEXER_QUEUE_NAME,
    DYNAMODB_TABLE_NAME
)

# Celery configuration with SQS and DynamoDB
CELERY_BROKER = f"sqs://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}@{AWS_REGION}"
CELERY_BACKEND = f"dynamodb://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}@{AWS_REGION}/{DYNAMODB_TABLE_NAME}"

# Import and setup AWS resources
try:
    from aws_config import setup_aws_resources
    if not setup_aws_resources():
        logger.warning("Failed to set up some AWS resources. Some functionality may be limited.")
except ImportError:
    logger.error("Failed to import aws_config. AWS services setup failed.")