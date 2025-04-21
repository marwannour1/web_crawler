#!/usr/bin/env python3
# filepath: run_indexer.py

"""
Indexer node script: runs Elasticsearch and indexer workers
"""
import os
import sys
import time
import signal
import logging
import subprocess
import threading
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from crawler_config import CrawlerConfig
from distributed_config import REDIS_URL, NODE_TYPE

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
            # Check if Elasticsearch is running
            es_status = check_elasticsearch()
            if es_status:
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Indexer node is running with Elasticsearch")
            else:
                self.send_response(503)  # Service Unavailable
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Elasticsearch not available")
        else:
            self.send_response(404)
            self.end_headers()

    # Silence log messages
    def log_message(self, format, *args):
        return

def check_elasticsearch():
    """Check if Elasticsearch is running"""
    try:
        response = requests.get('http://localhost:9200/_cluster/health', timeout=5)
        return response.status_code == 200
    except:
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
    """Start indexer workers connecting to the master's Redis"""
    config = CrawlerConfig().get_config()
    num_workers = config.get('num_indexers', 2)

    logger.info(f"Starting {num_workers} indexer workers connected to {REDIS_URL}")

    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.abspath(os.path.dirname(__file__))

    # Indexer workers - only handle tasks in indexer queue
    worker_process = subprocess.Popen([
        'celery', '-A', 'celery_app', 'worker',
        '--loglevel=info',
        '--concurrency', str(num_workers),
        '-Q', 'indexer',  # Only process indexer tasks
        '-n', f'indexer@{NODE_TYPE}',
        '-P', 'solo'
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

def main():
    """Main function for indexer node"""
    logger.info("Starting Indexer Node")

    # Check Elasticsearch
    if not check_elasticsearch():
        logger.warning("Elasticsearch is not running or not accessible.")
        logger.warning("Indexer workers will start anyway but may fail.")

    # Start health server in a background thread
    health_thread = threading.Thread(target=start_health_server)
    health_thread.daemon = True
    health_thread.start()

    # Start indexer workers in the main thread
    start_indexer_workers()

if __name__ == "__main__":
    main()