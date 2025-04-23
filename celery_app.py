#!/usr/bin/env python3
# filepath: celery_app.py

"""
Celery application configuration for the distributed web crawler.
Uses SQS as message broker and DynamoDB as result backend.
"""
from celery import Celery
import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Create Celery app
try:
    # Use celery_sqs.py for SQS integration
    from celery_sqs import create_celery_app
    app = create_celery_app()

    if app is None:
        raise ValueError("Failed to create Celery app with SQS configuration")
except ImportError as e:
    logger.error(f"Failed to import celery_sqs: {e}")
    sys.exit(1)
except ValueError as e:
    logger.error(f"Error creating Celery app: {e}")
    sys.exit(1)

# Register the DynamoDB backend
try:
    import aws_dynamodb_backend
    logger.info("DynamoDB backend registered successfully")
except ImportError as e:
    logger.error(f"Failed to import DynamoDB backend: {e}")
    sys.exit(1)

if __name__ == '__main__':
    app.start()