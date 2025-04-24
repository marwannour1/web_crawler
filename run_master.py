#!/usr/bin/env python3
# filepath: run_master.py

"""
Master node script: initializes AWS services and coordinates the crawl process.
The master node is responsible for managing the overall crawler process.
"""
import os
import sys
import time
import requests
import threading
import logging
from crawler_config import CrawlerConfig
from distributed_config import CRAWLER_IP, INDEXER_IP, MASTER_IP
from aws_config import setup_aws_resources, fix_dynamodb_table

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

def monitor_tasks_without_inspector(task_ids, max_runtime=300, status_interval=5):
    """Monitor task progress without using AsyncResult (SQS compatible)"""
    print(f"\nMonitoring {len(task_ids)} crawler tasks...")

    try:
        from celery_app import app as celery_app

        # Monitor task progress with overall timeout
        start_time = time.time()
        last_active_check = 0

        # Track tasks without using AsyncResult
        inspector = celery_app.control.inspect()

        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time

            # Check timeouts
            if elapsed_time > max_runtime:
                print("\nMaximum runtime exceeded. Stopping monitoring.")
                break

            # Only query active tasks every 5 seconds to reduce API calls
            if current_time - last_active_check >= status_interval:
                last_active_check = current_time

                # Get active and reserved tasks from all workers
                active_tasks = inspector.active() or {}
                reserved_tasks = inspector.reserved() or {}

                # Count active and reserved tasks
                active_count = sum(len(tasks) for tasks in active_tasks.values())
                reserved_count = sum(len(tasks) for tasks in reserved_tasks.values())

                # Display status
                print(f"\r[{time.strftime('%H:%M:%S')}] Tasks: {reserved_count} pending, " +
                      f"{active_count} active", end="", flush=True)

                # If no activity for a while and some time has passed, assume completion
                if active_count == 0 and reserved_count == 0 and elapsed_time > 60:
                    print("\nNo active or reserved tasks. Assuming completion.")
                    break

            # Short sleep to prevent high CPU usage
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nError in monitoring: {e}")

def main():
    """Main entry point for master node"""
    logger.info("Starting Web Crawler Master Node using AWS services")

    # Check if required environment variables are set
    if not check_environment_variables():
        sys.exit(1)

    # Initialize AWS resources
    logger.info("Initializing AWS resources...")
    if not setup_aws_resources():
        logger.error("Failed to setup AWS resources. Exiting.")
        sys.exit(1)

    # Fix DynamoDB table if needed
    fix_dynamodb_table()

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