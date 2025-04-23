#!/usr/bin/env python3
# filepath: run_master.py

"""
Master node script: initializes AWS services and coordinates the crawl process
"""
import os
import sys
import time
import requests
import threading
import logging
from crawler_config import CrawlerConfig
from distributed_config import CRAWLER_IP, INDEXER_IP, MASTER_IP

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("master.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_aws_resources():
    """Initialize AWS resources (SQS queues, DynamoDB table, S3 bucket)"""
    try:
        # Import here to avoid circular imports
        from aws_config import setup_aws_resources as aws_setup

        logger.info("Initializing AWS resources...")
        if aws_setup():
            logger.info("AWS resources initialized successfully")
            return True
        else:
            logger.error("Failed to initialize AWS resources")
            return False
    except Exception as e:
        logger.error(f"Error initializing AWS resources: {e}")
        return False

def health_check_worker():
    """Run periodic health checks on worker nodes"""
    while True:
        try:
            # Check crawler node health
            try:
                crawler_resp = requests.get(f"http://{CRAWLER_IP}:8080/health", timeout=5)
                crawler_status = "OK" if crawler_resp.status_code == 200 else "ERROR"
            except Exception:
                crawler_status = "DOWN"

            # Check indexer node health
            try:
                indexer_resp = requests.get(f"http://{INDEXER_IP}:8080/health", timeout=5)
                indexer_status = "OK" if indexer_resp.status_code == 200 else "ERROR"
            except Exception:
                indexer_status = "DOWN"

            logger.info(f"Node Status - Crawler: {crawler_status}, Indexer: {indexer_status}")

        except Exception as e:
            logger.error(f"Error in health check: {e}")

        # Sleep for 30 seconds before next check
        time.sleep(30)

def check_environment_variables():
    """Check if required AWS environment variables are set"""
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these environment variables before running the master node")
        return False

    # Check OpenSearch variables if endpoint is provided
    if os.environ.get("OPENSEARCH_ENDPOINT"):
        opensearch_vars = ["OPENSEARCH_USER", "OPENSEARCH_PASS"]
        missing_opensearch = [var for var in opensearch_vars if not os.environ.get(var)]
        if missing_opensearch:
            logger.warning(f"OpenSearch endpoint set but missing credentials: {', '.join(missing_opensearch)}")

    return True

def main():
    """Main entry point for master node"""
    logger.info("Starting Web Crawler Master Node using AWS services")

    # Check if required environment variables are set
    if not check_environment_variables():
        sys.exit(1)

    # Initialize AWS resources
    if not setup_aws_resources():
        logger.error("Failed to setup AWS resources. Continuing with limited functionality.")

    # Start health check thread
    health_thread = threading.Thread(target=health_check_worker)
    health_thread.daemon = True
    health_thread.start()
    logger.info("Health check monitoring started")

    # Start CLI interface
    try:
        from crawler_cli import main as cli_main
        logger.info("Starting CLI interface")
        cli_main()
    except KeyboardInterrupt:
        logger.info("Master node stopped by user")
    except Exception as e:
        logger.error(f"Error in CLI interface: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()