#!/usr/bin/env python3
# filepath: run_indexer.py

"""
Indexer node script: handles content indexing to OpenSearch and S3 storage
"""
import os
import sys
import time
import signal
import logging
import subprocess
import requests
import threading
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from crawler_config import CrawlerConfig
from distributed_config import NODE_TYPE
from aws_config import setup_aws_resources
from requests_aws4auth import AWS4Auth
import tasks

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("indexer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks"""
    def do_GET(self):
        if self.path == '/health':
            # Check if OpenSearch is available
            es_status = check_opensearch()
            if es_status:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "ok",
                    "message": "Indexer node is running with OpenSearch service",
                    "node_type": NODE_TYPE
                }).encode())
            else:
                self.send_response(503)  # Service Unavailable
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "message": "OpenSearch service not available",
                    "node_type": NODE_TYPE
                }).encode())
        elif self.path == '/status':
            # Return more detailed status information
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            # Get task stats if available
            task_stats = {"pending": "unknown", "active": "unknown"}
            try:
                from celery_app import app
                inspector = app.control.inspect()
                active = inspector.active()
                reserved = inspector.reserved()

                active_count = 0
                if active:
                    active_count = sum(len(tasks) for tasks in active.values())

                reserved_count = 0
                if reserved:
                    reserved_count = sum(len(tasks) for tasks in reserved.values())

                task_stats = {"active": active_count, "pending": reserved_count}
            except Exception as e:
                logger.error(f"Error getting task stats: {e}")

            self.wfile.write(json.dumps({
                "status": "ok",
                "node_type": NODE_TYPE,
                "uptime": time.time() - start_time,
                "opensearch_status": "available" if check_opensearch() else "unavailable",
                "task_stats": task_stats
            }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    # Silence log messages
    def log_message(self, format, *args):
        return

    def do_POST(self):
        if self.path == '/shutdown':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Shutting down crawler node")

            # Schedule the shutdown after responding to the request
            threading.Thread(target=self._shutdown).start()
        else:
            self.send_response(404)
            self.end_headers()

    def _shutdown(self):
        """Shutdown the node gracefully after a short delay"""
        time.sleep(1)  # Give the response time to complete
        os.kill(os.getpid(), signal.SIGTERM)  # Send SIGTERM to current process

def check_opensearch():
    """Check if AWS OpenSearch is running and accessible"""
    try:
        # Try to import specific configuration
        try:
            from distributed_config import OPENSEARCH_ENDPOINT, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
            from distributed_config import OPENSEARCH_USER, OPENSEARCH_PASS

            if not OPENSEARCH_ENDPOINT:
                logger.warning("OpenSearch endpoint not defined")
                return False

            # Determine authentication method from file if available
            auth_method = "aws4auth"  # Default
            try:
                with open("opensearch_auth_method.txt", "r") as f:
                    auth_method = f.read().strip()
            except (FileNotFoundError, IOError):
                pass

            if auth_method == "aws4auth":
                # Use AWS4Auth for OpenSearch
                aws_auth = AWS4Auth(
                    AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY,
                    AWS_REGION,
                    'es'
                )
                response = requests.get(
                    f"{OPENSEARCH_ENDPOINT}/_cluster/health",
                    auth=aws_auth,
                    timeout=5,
                    verify=True
                )
            else:
                # Fall back to basic auth
                response = requests.get(
                    f"{OPENSEARCH_ENDPOINT}/_cluster/health",
                    auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
                    timeout=5,
                    verify=True
                )

            logger.info(f"OpenSearch health check: {response.status_code}")
            return response.status_code == 200
        except ImportError as e:
            logger.error(f"Failed to import OpenSearch configuration: {e}")
            return False
    except Exception as e:
        logger.error(f"OpenSearch health check failed: {e}")
        return False

def start_health_server():
    """Start HTTP server for health checks"""
    try:
        server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
        logger.info("Health check server started on port 8080")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start health server: {e}")

def start_indexer_workers():
    """Start indexer workers connecting to AWS SQS"""
    config = CrawlerConfig().get_config()
    num_workers = config.get('num_indexers', 2)

    logger.info(f"Starting {num_workers} indexer workers connected to AWS SQS")



    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.abspath(os.path.dirname(__file__))

    # Indexer workers - only handle tasks in indexer queue
    worker_process = subprocess.Popen([
        'python3', 'worker_launch.py',
        '--loglevel=info',
        '--concurrency', str(num_workers),
        '-Q', 'indexer',  # Only process indexer tasks
        '-n', f'indexer@{NODE_TYPE}',
        '-P', 'solo',
        '--without-gossip',
        '--without-mingle',
        '--without-heartbeat'
    ], env=env)

    logger.info("Indexer workers started successfully")

    # Handle graceful shutdown
    def terminate_workers(signum, frame):
        logger.info("Shutting down workers...")
        worker_process.terminate()
        sys.exit(0)

    signal.signal(signal.SIGTERM, terminate_workers)
    signal.signal(signal.SIGINT, terminate_workers)

    try:
        worker_process.wait()
    except KeyboardInterrupt:
        worker_process.terminate()
        sys.exit(0)

def setup_opensearch_auth():
    """Determine the best authentication method for OpenSearch"""
    try:
        from aws_config import test_opensearch_connection
        success, auth_method = test_opensearch_connection()
        if success:
            logger.info(f"OpenSearch connection successful using {auth_method} authentication")
            return True
        else:
            logger.warning("Failed to connect to OpenSearch with any authentication method")
            return False
    except ImportError as e:
        logger.error(f"Failed to import opensearch_fix: {e}")
        return False

# Track start time for uptime monitoring
start_time = time.time()

def main():
    """Main function for indexer node"""
    logger.info("Starting Indexer Node")

    # Initialize AWS resources
    if not setup_aws_resources():
        logger.warning("Some AWS resources could not be initialized")

    # Check if using AWS OpenSearch
    try:
        from distributed_config import OPENSEARCH_ENDPOINT
        using_aws = bool(OPENSEARCH_ENDPOINT)
        if using_aws:
            logger.info(f"Using AWS OpenSearch Service at {OPENSEARCH_ENDPOINT}")
            # Setup authentication method
            setup_opensearch_auth()
        else:
            logger.warning("No OpenSearch endpoint configured. Indexing will use S3 only.")
    except ImportError:
        logger.warning("Distributed configuration not found. Indexing will be limited.")

    # Start health server in a background thread
    health_thread = threading.Thread(target=start_health_server)
    health_thread.daemon = True
    health_thread.start()

    # Start indexer workers in the main thread
    start_indexer_workers()

if __name__ == "__main__":
    main()