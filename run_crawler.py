#!/usr/bin/env python3
# filepath: run_crawler.py

"""
Crawler node script: runs crawler workers and provides health status endpoint
"""
import os
import sys
import time
import signal
import logging
import subprocess
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from crawler_config import CrawlerConfig
from distributed_config import REDIS_URL, NODE_TYPE

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks"""
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Crawler node is running")
        else:
            self.send_response(404)
            self.end_headers()

    # Silence log messages
    def log_message(self, format, *args):
        return

def start_health_server():
    """Start HTTP server for health checks"""
    try:
        server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
        logger.info("Health check server started on port 8080")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start health server: {e}")

def start_crawler_workers():
    """Start crawler workers connecting to the master's Redis"""
    config = CrawlerConfig().get_config()
    num_workers = config.get('num_crawlers', 4)

    logger.info(f"Starting {num_workers} crawler workers connected to {REDIS_URL}")

    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.abspath(os.path.dirname(__file__))

    # Crawler workers - only handle tasks in crawler queue
    worker_process = subprocess.Popen([
        'celery', '-A', 'celery_app', 'worker',
        '--loglevel=info',
        '--concurrency', str(num_workers),
        '-Q', 'crawler',  # Only process crawler tasks
        '-n', f'crawler@{NODE_TYPE}',
        '-P', 'solo'
    ], env=env)

    logger.info("Crawler workers started successfully")

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
    """Main function for crawler node"""
    logger.info("Starting Crawler Node")

    # Start health server in a background thread
    health_thread = threading.Thread(target=start_health_server)
    health_thread.daemon = True
    health_thread.start()

    # Start crawler workers in the main thread
    start_crawler_workers()

if __name__ == "__main__":
    main()