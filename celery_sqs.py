#!/usr/bin/env python3
# filepath: celery_sqs.py

"""
SQS and DynamoDB integration for Celery.
Sets up Celery to use SQS as broker and DynamoDB as result backend.
"""

import boto3
import logging
import os
from celery import Celery
from aws_config import (
    AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY,
    SQS_CRAWLER_QUEUE_NAME, SQS_INDEXER_QUEUE_NAME,
    DYNAMODB_TABLE_NAME, ensure_aws_clients
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_celery_app():
    """Create and configure a Celery app using SQS and DynamoDB"""
    from aws_config import get_crawler_queue_url, get_indexer_queue_url

    # Make sure AWS clients are initialized
    ensure_aws_clients()

    # SQS broker URL
    broker_url = f"sqs://{AWS_ACCESS_KEY}:{AWS_SECRET_KEY}@"

    # Create Celery app
    app = Celery('webcrawler', broker=broker_url)

    # Get queue URLs
    crawler_queue_url = get_crawler_queue_url()
    indexer_queue_url = get_indexer_queue_url()

    if not crawler_queue_url or not indexer_queue_url:
        logger.error("Failed to get SQS queue URLs. Check AWS credentials and region.")
        return None

    # Configure Celery to use SQS
    app.conf.update(
        broker_transport_options={
            'region': AWS_REGION,
            'predefined_queues': {
                'crawler': {
                    'name': SQS_CRAWLER_QUEUE_NAME,
                    'visibility_timeout': 300,  # 5 minutes
                    'url': crawler_queue_url
                },
                'indexer': {
                    'name': SQS_INDEXER_QUEUE_NAME,
                    'visibility_timeout': 300,
                    'url': indexer_queue_url
                }
            },
            'queue_name_prefix': 'webcrawler-'  # Use this for auto-created queues
        },
        task_default_queue='crawler',
        broker_connection_retry_on_startup=True,

        # Disable control features to avoid queue creation issues in SQS
        worker_enable_remote_control=False,
        worker_send_task_events=False,
        task_send_sent_event=False,
        worker_disable_rate_limits=True,
        task_track_started=False
    )

    # Configure result backend (DynamoDB)
    app.conf.update(
        result_backend=f"dynamodb://{AWS_ACCESS_KEY}:{AWS_SECRET_KEY}@{AWS_REGION}/{DYNAMODB_TABLE_NAME}"
    )

    # Automatic routing of tasks to queues
    app.conf.task_routes = {
        'tasks.crawl': {'queue': 'crawler'},
        'tasks.index': {'queue': 'indexer'},
    }

    return app