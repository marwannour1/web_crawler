#!/usr/bin/env python3
# filepath: fix_dynamodb.py

"""
Fix DynamoDB table for the web crawler.
This script will properly create the DynamoDB table with TTL.
"""

import boto3
import time
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DynamoDB Configuration
DYNAMODB_TABLE_NAME = "webcrawler-tasks"

def fix_dynamodb_table():
    """Fix the DynamoDB table schema to work with Celery"""
    try:
        # Create session with credentials from environment
        session = boto3.Session()
        dynamodb_client = session.client('dynamodb')

        # Check if table exists
        try:
            dynamodb_client.describe_table(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"Table {DYNAMODB_TABLE_NAME} exists, deleting it to recreate...")

            # Delete existing table
            dynamodb_client.delete_table(TableName=DYNAMODB_TABLE_NAME)

            # Wait for the table to be deleted
            logger.info(f"Waiting for table {DYNAMODB_TABLE_NAME} to be deleted...")
            waiter = dynamodb_client.get_waiter('table_not_exists')
            waiter.wait(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"Table {DYNAMODB_TABLE_NAME} deleted successfully")

        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                logger.error(f"Error checking table: {e}")
            else:
                logger.info(f"Table {DYNAMODB_TABLE_NAME} does not exist, will create it")

        # Create the table with the correct schema for Celery
        logger.info(f"Creating DynamoDB table {DYNAMODB_TABLE_NAME}...")
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
        logger.info(f"Waiting for table {DYNAMODB_TABLE_NAME} to be active...")
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=DYNAMODB_TABLE_NAME)
        logger.info(f"Table {DYNAMODB_TABLE_NAME} created successfully")

        # Add a sleep to ensure the table is fully active
        time.sleep(5)

        # Enable TTL on the new table
        try:
            logger.info(f"Enabling TTL on table {DYNAMODB_TABLE_NAME}...")
            dynamodb_client.update_time_to_live(
                TableName=DYNAMODB_TABLE_NAME,
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': 'expires'  # Use 'expires' instead of 'expires_at'
                }
            )
            logger.info(f"TTL enabled successfully on {DYNAMODB_TABLE_NAME}")
        except ClientError as e:
            if "TimeToLive is already enabled" in str(e):
                logger.info(f"TTL is already enabled for {DYNAMODB_TABLE_NAME}")
            else:
                logger.warning(f"Could not enable TTL: {e}")
                logger.warning("This is not critical - the crawler will still work")

        return True

    except Exception as e:
        logger.error(f"Error fixing DynamoDB table: {e}")
        return False

if __name__ == "__main__":
    print("Fixing DynamoDB table for web crawler...")
    if fix_dynamodb_table():
        print("✅ DynamoDB table fixed successfully!")
    else:
        print("❌ Failed to fix DynamoDB table!")