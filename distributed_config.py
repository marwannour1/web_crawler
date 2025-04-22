#!/usr/bin/env python3
# filepath: distributed_config.py

"""
Configuration for distributed web crawler deployment on AWS
"""
import os

# Node IP addresses - update these with your actual EC2 IP addresses
MASTER_IP = "172.31.25.56"
CRAWLER_IP = "172.31.18.178"
INDEXER_IP = "172.31.21.25"

# AWS OpenSearch Service configuration
OPENSEARCH_ENDPOINT = os.environ.get("OPENSEARCH_ENDPOINT", "")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER", "elastic")
OPENSEARCH_PASS = os.environ.get("OPENSEARCH_PASS", "")

# Use AWS OpenSearch if endpoint is provided, otherwise use local setup
if OPENSEARCH_ENDPOINT:
    ELASTICSEARCH_URL = OPENSEARCH_ENDPOINT
else:
    ELASTICSEARCH_URL = f"http://{INDEXER_IP}:9200"

# Service configurations
REDIS_URL = f"redis://{MASTER_IP}:6379/0"

# Node type - set appropriately on each instance
NODE_TYPE = os.environ.get("NODE_TYPE", "master")  # Change to "crawler" or "indexer" on respective nodes

# Celery configuration
CELERY_BROKER = REDIS_URL
CELERY_BACKEND = REDIS_URL