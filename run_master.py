#!/usr/bin/env python3
# filepath: run_master.py

"""
Master node script: initializes AWS services and coordinates the crawl process.
The master node is responsible for managing the overall crawler process.
"""
import os
import sys
import time
import json  # Missing import for JSON
import requests
import threading
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer  # Missing import for HTTP server
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

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks"""
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "ok",
                "message": "Master node is running",
                "node_type": "master"
            }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    # Override log methods to reduce noise
    def log_message(self, format, *args):
        if "/health" not in args[0]:  # Don't log health check requests
            logger.info("%s - %s" % (self.address_string(), format % args))

def start_health_server():
    """Start HTTP server for health checks"""
    server_address = ('', 8080)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info("Starting health check server on port 8080")
    httpd.serve_forever()

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

def monitor_tasks_without_inspector(task_ids, max_runtime=1200, status_interval=5):
    """Monitor task progress without using Celery inspector (SQS compatible)"""
    print(f"\nMonitoring {len(task_ids)} crawler tasks...")

    from crawler_cli import monitor_tasks
    monitor_tasks(task_ids, max_runtime, status_interval)

def count_s3_objects(prefix):
    """Count objects in S3 with a given prefix"""
    from aws_config import S3_BUCKET_NAME, s3_client
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix
        )
        return response.get('KeyCount', 0)
    except Exception as e:
        print(f"Error counting S3 objects: {e}")
        return 0

def main():
    """Main entry point for master node"""
    logger.info("Starting Web Crawler Master Node using AWS services")

    # Start health server in a background thread
    health_server_thread = threading.Thread(target=start_health_server)  # Renamed for clarity
    health_server_thread.daemon = True
    health_server_thread.start()
    logger.info("Health check server started")

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
    health_monitor_thread = threading.Thread(target=health_check_worker)  # Renamed for clarity
    health_monitor_thread.daemon = True
    health_monitor_thread.start()
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