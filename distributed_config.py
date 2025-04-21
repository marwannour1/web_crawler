#!/usr/bin/env python3
# filepath: distributed_config.py

"""
Configuration for distributed web crawler deployment on AWS
"""

# Node IP addresses - update these with your actual EC2 IP addresses
MASTER_IP = "172.31.25.56 "  # Replace with your Master node's IP
CRAWLER_IP = "172.31.18.178"  # Replace with your Crawler node's IP
INDEXER_IP = "172.31.28.247"  # Replace with your Indexer node's IP

# Service configurations
REDIS_URL = f"redis://{MASTER_IP}:6379/0"
ELASTICSEARCH_URL = f"http://{INDEXER_IP}:9200"

# Node type - set appropriately on each instance
NODE_TYPE = "master"  # Change to "crawler" or "indexer" on respective nodes

# Celery configuration
CELERY_BROKER = REDIS_URL
CELERY_BACKEND = REDIS_URL