#!/usr/bin/env python3
# filepath: celery_app.py

"""
Celery application configuration for the distributed web crawler.
Uses SQS as message broker and DynamoDB as result backend.
"""
from celery import Celery
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Create Celery app
try:
    # Try to use celery_sqs.py for SQS integration
    from celery_sqs import create_celery_app
    app = create_celery_app()
except ImportError:
    # Fallback to standard configuration
    app = Celery('web_crawler')
    app.config_from_object('celeryconfig')

# Register the DynamoDB backend
try:
    import aws_dynamodb_backend
except ImportError:
    pass

# This ensures tasks are loaded when Celery starts
try:
    import tasks
except ImportError:
    print("Error importing tasks. Make sure the tasks module is available.")
    sys.exit(1)

if __name__ == '__main__':
    app.start()