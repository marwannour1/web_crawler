#!/usr/bin/env python3

import boto3
import os
import json
import logging
from botocore.exceptions import ClientError
from aws_config import AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY, DYNAMODB_TABLE_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_dynamodb_table():
    """Fix the DynamoDB table schema to work with Celery"""
    try:
        # Create DynamoDB client
        client = boto3.client(
            'dynamodb',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        # Check if table exists
        try:
            client.describe_table(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"Table {DYNAMODB_TABLE_NAME} exists, deleting it to recreate with correct schema")

            # Delete the existing table
            client.delete_table(TableName=DYNAMODB_TABLE_NAME)

            # Wait for the table to be deleted
            waiter = client.get_waiter('table_not_exists')
            waiter.wait(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"Table {DYNAMODB_TABLE_NAME} deleted successfully")

        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                logger.error(f"Error checking table: {e}")
                return False

        # Create the table with the correct schema for Celery
        client.create_table(
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
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName=DYNAMODB_TABLE_NAME)

        # Enable TTL on the new table
        client.update_time_to_live(
            TableName=DYNAMODB_TABLE_NAME,
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': 'expires'  # Use 'expires' instead of 'expires_at'
            }
        )

        logger.info(f"Table {DYNAMODB_TABLE_NAME} recreated successfully with correct schema")
        return True

    except Exception as e:
        logger.error(f"Error fixing DynamoDB table: {e}")
        return False

if __name__ == "__main__":
    fix_dynamodb_table()