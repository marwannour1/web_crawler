#!/usr/bin/env python3

import boto3
import requests
from requests_aws4auth import AWS4Auth
import os
import json
import logging
from distributed_config import OPENSEARCH_ENDPOINT, OPENSEARCH_USER, OPENSEARCH_PASS, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_opensearch_connection():
    """Test and fix OpenSearch authentication"""
    try:
        # Try AWS4Auth first (best for AWS OpenSearch)
        auth = AWS4Auth(
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_REGION,
            'es'  # service name for OpenSearch
        )

        # Test connection with AWS4Auth
        response = requests.get(
            f"{OPENSEARCH_ENDPOINT}/_cluster/health",
            auth=auth,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 200:
            logger.info("✅ AWS4Auth connection to OpenSearch successful!")
            logger.info(f"Cluster health: {response.json()}")
            return True, "aws4auth"
        else:
            logger.warning(f"AWS4Auth failed with status code {response.status_code}")

        # Try basic auth
        response = requests.get(
            f"{OPENSEARCH_ENDPOINT}/_cluster/health",
            auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 200:
            logger.info("✅ Basic auth connection to OpenSearch successful!")
            logger.info(f"Cluster health: {response.json()}")
            return True, "basic"
        else:
            logger.error(f"Basic auth failed with status code {response.status_code}")

        logger.error("All authentication methods failed for OpenSearch")
        return False, None

    except Exception as e:
        logger.error(f"Error testing OpenSearch connection: {e}")
        return False, None

if __name__ == "__main__":
    success, auth_method = test_opensearch_connection()
    if success:
        print(f"\nSuccess! Use the '{auth_method}' method in your configuration.")

        # Create a helper file for easy auth method switching
        with open("opensearch_auth_method.txt", "w") as f:
            f.write(auth_method)
    else:
        print("\nFailed to connect to OpenSearch. Check your credentials and network settings.")