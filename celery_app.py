#!/usr/bin/env python3
# filepath: celery_app.py

import os
import logging
from celery import Celery
from urllib.parse import quote
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Try to get AWS credentials directly from environment variables
    from aws_config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, DYNAMODB_TABLE_NAME

    # Set up SQS broker URL (using environment variables)
    broker_url = f"sqs://{AWS_ACCESS_KEY}:{quote(AWS_SECRET_KEY)}@"

    # Create the Celery app with SQS broker
    app = Celery('webcrawler', broker=broker_url)

    # Direct backend configuration - bypass registration system
    from aws_dynamodb_backend import DynamoDBBackend

    # Configure the app
    app.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_track_started=True,
        broker_transport_options={
            'region': AWS_REGION,
            'polling_interval': 5,  # Seconds between polling SQS
            'wait_time_seconds': 20,  # Max time for long polling
            'visibility_timeout': 300,  # 5 minutes visibility timeout
        },
        task_routes={
            'tasks.crawl': {'queue': 'crawler'},
            'tasks.index': {'queue': 'indexer'},
        }

    )

    # Directly instantiate the backend
    app.backend = DynamoDBBackend(
        aws_access_key=AWS_ACCESS_KEY,
        aws_secret_key=AWS_SECRET_KEY,
        region=AWS_REGION,
        table_name=DYNAMODB_TABLE_NAME
    )

    logger.info("Celery app initialized with SQS broker and DynamoDB backend")

except ImportError as e:
    logger.error(f"Failed to import AWS configuration: {e}")
    # Fallback to local Redis for development
    app = Celery('webcrawler', broker='redis://localhost:6379/0',
                 backend='redis://localhost:6379/0')
    logger.warning("Using local Redis backend (development mode)")

except NoCredentialsError:
    logger.error("AWS credentials not found")
    app = Celery('webcrawler', broker='redis://localhost:6379/0',
                 backend='redis://localhost:6379/0')
    logger.warning("Using local Redis backend (AWS credentials missing)")

except Exception as e:
    logger.error(f"Failed to initialize DynamoDB backend: {e}")
    # Final fallback to local backend
    app = Celery('webcrawler', broker='redis://localhost:6379/0',
                 backend='redis://localhost:6379/0')
    logger.warning(f"Using local Redis backend due to error: {e}")