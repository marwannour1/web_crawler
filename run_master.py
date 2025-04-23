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
    """Monitor task progress without using Celery inspector (SQS compatible)"""
    print(f"\nMonitoring {len(task_ids)} crawler tasks...")

    try:
        from celery.result import AsyncResult
        from celery_app import app as celery_app

        # Get initial tasks
        initial_tasks = [AsyncResult(task_id, app=celery_app) for task_id in task_ids]

        # Monitor task progress with overall timeout
        start_time = time.time()

        while True:
            # Only check the state of the tasks we're explicitly tracking
            states = [task.state for task in initial_tasks]

            pending_count = states.count('PENDING')
            running_count = states.count('STARTED')
            success_count = states.count('SUCCESS')
            failed_count = states.count('FAILURE')

            total_active = pending_count + running_count

            print(f"\r[{time.strftime('%H:%M:%S')}] Tasks: {pending_count} pending, " +
                  f"{running_count} running, {success_count} completed, {failed_count} failed",
                  end="", flush=True)

            # Check if all initial tasks are complete
            all_initial_done = all(task.ready() for task in initial_tasks)

            # Check timeouts
            elapsed_time = time.time() - start_time
            if elapsed_time > max_runtime:
                print("\nMaximum runtime exceeded. Stopping monitoring.")
                break

            if all_initial_done and total_active == 0:
                print("\nAll tracked tasks completed.")
                break

            # Check if we're getting results for seed tasks
            if elapsed_time > 60 and all(task.state == 'PENDING' for task in initial_tasks):
                print("\nWarning: Initial tasks still pending after 60 seconds.")
                print("Workers may not be processing tasks. Check crawler and indexer nodes.")
                break

            time.sleep(status_interval)

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