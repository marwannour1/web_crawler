#!/usr/bin/env python3
# filepath: aws_dynamodb_backend.py

"""
Custom DynamoDB result backend for Celery.
"""

import boto3
import json
import time
import logging
import base64
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from celery.backends.base import KeyValueStoreBackend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DynamoDBBackend(KeyValueStoreBackend):
    """DynamoDB backend for Celery."""

    def __init__(self, url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse the URL to get credentials and table name
        if url:
            # Expected format: dynamodb://access_key:secret_key@region/table_name
            parts = url.split('://', 1)[1].split('@')
            credentials = parts[0].split(':')
            region_table = parts[1].split('/')

            self.aws_access_key = credentials[0]
            self.aws_secret_key = credentials[1]
            self.region_name = region_table[0]
            self.table_name = region_table[1]
        else:
            # Default values (not recommended)
            self.aws_access_key = kwargs.get('aws_access_key', None)
            self.aws_secret_key = kwargs.get('aws_secret_key', None)
            self.region_name = kwargs.get('region', 'us-east-1')
            self.table_name = kwargs.get('table_name', 'celery_tasks')

        # TTL for results (default: 1 day)
        self.expires = kwargs.get('expires', 86400)

        # Initialize DynamoDB client
        self.client = boto3.client(
            'dynamodb',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region_name
        )

    def _get_table_if_exists(self):
        """Check if the table exists"""
        try:
            self.client.describe_table(TableName=self.table_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            raise

    def _create_table(self):
        """Create the DynamoDB table if it doesn't exist"""
        if self._get_table_if_exists():
            return True

        try:
            self.client.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'}  # Use 'id' instead of 'task_id'
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

            # Wait for table creation
            waiter = self.client.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)

            # Enable TTL
            self.client.update_time_to_live(
                TableName=self.table_name,
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': 'expires'  # Use 'expires' instead of 'expires_at'
                }
            )

            return True
        except Exception as e:
            logger.error(f"Error creating DynamoDB table: {e}")
            return False

    def _store_result(self, key, result, state, traceback=None):
        """Store a task result in DynamoDB"""
        try:
            # Ensure the table exists
            if not self._get_table_if_exists():
                self._create_table()

            # Calculate expiry time (TTL)
            expires_at = int(time.time()) + self.expires

            # Prepare the result
            if result is not None:
                result = self.encode(result)

            # Store in DynamoDB - use 'id' instead of 'task_id'
            item = {
                'id': {'S': key},
                'state': {'S': state},
                'result': {'S': result} if result else {'NULL': True},
                'expires': {'N': str(expires_at)}  # Use 'expires' instead of 'expires_at'
            }

            if traceback:
                item['traceback'] = {'S': traceback}

            self.client.put_item(
                TableName=self.table_name,
                Item=item
            )

            return result
        except Exception as e:
            logger.error(f"Error storing result in DynamoDB: {e}")
            return None

    def _get_result(self, key):
        """Get a task result from DynamoDB"""
        try:
            response = self.client.get_item(
                TableName=self.table_name,
                Key={'id': {'S': key}}  # Use 'id' instead of 'task_id'
            )

            if 'Item' not in response:
                return None

            item = response['Item']

            # Extract result and state
            state = item.get('state', {}).get('S')

            if 'result' in item and 'S' in item['result']:
                result = self.decode(item['result']['S'])
            else:
                result = None

            return {'result': result, 'state': state}
        except Exception as e:
            logger.error(f"Error getting result from DynamoDB: {e}")
            return None

    def _delete_result(self, key):
        """Delete a task result from DynamoDB"""
        try:
            self.client.delete_item(
                TableName=self.table_name,
                Key={'id': {'S': key}}  # Use 'id' instead of 'task_id'
            )
            return True
        except Exception as e:
            logger.error(f"Error deleting result from DynamoDB: {e}")
            return False

    def encode(self, data):
        """Encode data as JSON string"""
        return base64.b64encode(json.dumps(data).encode()).decode()

    def decode(self, data):
        """Decode JSON string"""
        return json.loads(base64.b64decode(data.encode()).decode())

# Register the backend
from celery.backends import register
register('dynamodb', DynamoDBBackend)