#!/usr/bin/env python3
# filepath: s3_storage.py

"""
S3 storage for crawled content.
Provides functions to save, retrieve, and manage crawled content in S3.
"""

import boto3
import hashlib
import json
import logging
import time
from botocore.exceptions import ClientError
from aws_config import AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME, S3_OUTPUT_PREFIX, S3_INPUT_PREFIX, ensure_aws_clients

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 client
def get_s3_client():
    """Get an S3 client with the configured credentials"""
    ensure_aws_clients()
    from aws_config import s3_client
    return s3_client

def save_to_s3(content, url):
    """Save crawled content to S3 output directory"""
    s3_client = get_s3_client()
    if not s3_client:
        return False

    # Generate unique key based on URL hash
    url_hash = hashlib.md5(url.encode()).hexdigest()
    key = f"{S3_OUTPUT_PREFIX}{url_hash}.json"

    # Add timestamp for tracking
    content['stored_timestamp'] = time.time()

    try:
        # Convert content dict to JSON
        json_content = json.dumps(content)

        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json_content.encode(),
            ContentType='application/json'
        )
        logger.info(f"Saved content to S3: {url} -> s3://{S3_BUCKET_NAME}/{key}")

        # Also save a text version for easy reading
        if 'text_content' in content:
            text_content = (
                f"URL: {content['url']}\n"
                f"Title: {content['title']}\n"
                f"Description: {content['description']}\n\n"
                f"{content['text_content']}"
            )
            text_key = f"{S3_OUTPUT_PREFIX}{url_hash}.txt"
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=text_key,
                Body=text_content.encode('utf-8'),
                ContentType='text/plain'
            )

        return key
    except Exception as e:
        logger.error(f"Error saving content to S3 for {url}: {e}")
        return None

def retrieve_from_s3(url=None, key=None, input_dir=False):
    """Retrieve content from S3 by URL or direct key"""
    s3_client = get_s3_client()
    if not s3_client:
        return None

    if not key and url:
        # Generate key from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()
        prefix = S3_INPUT_PREFIX if input_dir else S3_OUTPUT_PREFIX
        key = f"{prefix}{url_hash}.json"

    if not key:
        logger.error("Either URL or key must be provided")
        return None

    try:
        # Get object from S3
        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key=key
        )

        # Parse JSON content
        json_content = response['Body'].read().decode('utf-8')
        content = json.loads(json_content)

        return content
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.warning(f"Content not found in S3: {key}")
        else:
            logger.error(f"Error retrieving content from S3 for {key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing content from S3 for {key}: {e}")
        return None

def list_stored_content(input_dir=False):
    """List all stored content in the S3 bucket"""
    s3_client = get_s3_client()
    if not s3_client:
        return []

    prefix = S3_INPUT_PREFIX if input_dir else S3_OUTPUT_PREFIX

    try:
        # List objects in the bucket with the given prefix
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix
        )

        if 'Contents' not in response:
            return []

        # Extract .json files only (ignore .txt versions)
        return [item['Key'] for item in response['Contents'] if item['Key'].endswith('.json')]
    except Exception as e:
        logger.error(f"Error listing content in S3: {e}")
        return []

def check_content_exists(url, input_dir=False):
    """Check if content for a URL already exists in S3"""
    s3_client = get_s3_client()
    if not s3_client:
        return False

    # Generate key from URL
    url_hash = hashlib.md5(url.encode()).hexdigest()
    prefix = S3_INPUT_PREFIX if input_dir else S3_OUTPUT_PREFIX
    key = f"{prefix}{url_hash}.json"

    try:
        s3_client.head_object(
            Bucket=S3_BUCKET_NAME,
            Key=key
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logger.error(f"Error checking content existence in S3: {e}")
            return False

def upload_seed_urls_to_s3(urls):
    """Upload seed URLs to S3 input directory"""
    s3_client = get_s3_client()
    if not s3_client:
        return False

    try:
        # Create a JSON object with the URLs
        seed_data = {
            'seed_urls': urls,
            'timestamp': time.time()
        }

        json_content = json.dumps(seed_data)

        # Upload to S3
        key = f"{S3_INPUT_PREFIX}seed_urls.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json_content.encode(),
            ContentType='application/json'
        )

        logger.info(f"Uploaded {len(urls)} seed URLs to S3: s3://{S3_BUCKET_NAME}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error uploading seed URLs to S3: {e}")
        return False

def get_seed_urls_from_s3():
    """Get seed URLs from S3 input directory"""
    s3_client = get_s3_client()
    if not s3_client:
        return None

    try:
        # Get the seed URLs file
        key = f"{S3_INPUT_PREFIX}seed_urls.json"
        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key=key
        )

        # Parse JSON content
        json_content = response['Body'].read().decode('utf-8')
        seed_data = json.loads(json_content)

        return seed_data.get('seed_urls', [])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.warning(f"No seed URLs file found in S3: {key}")
        else:
            logger.error(f"Error retrieving seed URLs from S3: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing seed URLs from S3: {e}")
        return None