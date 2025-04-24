#!/usr/bin/env python3
# filepath: celery_app.py

import os
import logging
from celery import Celery
from urllib.parse import quote
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Try to get AWS credentials directly from environment variables
    from aws_config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, DYNAMODB_TABLE_NAME, SQS_CRAWLER_QUEUE_NAME, SQS_INDEXER_QUEUE_NAME

    # Set up SQS broker URL (using environment variables)
    broker_url = f"sqs://{AWS_ACCESS_KEY}:{quote(AWS_SECRET_KEY)}@"

    # Create the Celery app with SQS broker
    app = Celery('webcrawler', broker=broker_url)

    # Configure the app with AWS settings
    app.conf.update(
        # Basic celery settings
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_track_started=True,

        # Set result backend for simplicity
        result_backend='rpc://',
        task_ignore_result=True,

        worker_send_Task_events=False,
        worker_enable_remote_control=False,
        task_send_sent_event=False,

        # SQS broker configuration
        broker_transport_options={
            'region': AWS_REGION,
            'polling_interval': 5,  # Seconds between polling SQS
            'wait_time_seconds': 20,  # Max time for long polling
            'visibility_timeout': 300,  # 5 minutes visibility timeout
            'predefined_queues': {
                'crawler': {
                    'url': f"https://sqs.{AWS_REGION}.amazonaws.com/{SQS_CRAWLER_QUEUE_NAME}",
                    'access_key_id': AWS_ACCESS_KEY,
                    'secret_access_key': AWS_SECRET_KEY,
                },
                'indexer': {
                    'url': f"https://sqs.{AWS_REGION}.amazonaws.com/{SQS_INDEXER_QUEUE_NAME}",
                    'access_key_id': AWS_ACCESS_KEY,
                    'secret_access_key': AWS_SECRET_KEY,
                },
            }
        },

        # Route tasks to appropriate queues
        task_routes={
            'tasks.crawl': {'queue': 'crawler'},
            'tasks.index': {'queue': 'indexer'},
        }
    )

    logger.info("Celery app initialized with SQS broker")
    logger.info(f"Using {SQS_CRAWLER_QUEUE_NAME} for crawler tasks")
    logger.info(f"Using {SQS_INDEXER_QUEUE_NAME} for indexer tasks")

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
    logger.error(f"Failed to initialize AWS configuration: {e}")
    # Use memory backend instead of Redis as fallback
    app = Celery('webcrawler')
    app.conf.update(
        broker_url='memory://',
        result_backend='rpc://',
        task_ignore_result=True,
    )
    logger.warning(f"Using memory backend due to error: {e}")