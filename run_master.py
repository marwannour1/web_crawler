#!/usr/bin/env python3
# filepath: run_master.py

"""
Master node script: starts Redis and coordinates the crawl process
"""
import os
import sys
import time
import requests
import threading
import subprocess
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

def check_redis():
    """Ensure Redis is running"""
    try:
        result = subprocess.run(['redis-cli', 'ping'],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        if result.returncode != 0 or b'PONG' not in result.stdout:
            logger.error("Redis is not running. Starting Redis...")
            subprocess.Popen(['redis-server', '--daemonize', 'yes'])
            time.sleep(2)  # Give Redis time to start
            return check_redis()  # Verify it started
        return True
    except Exception as e:
        logger.error(f"Failed to check/start Redis: {e}")
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

def main():
    """Main entry point for master node"""
    logger.info("Starting Web Crawler Master Node")

    # Check if Redis is running
    if not check_redis():
        logger.error("Failed to ensure Redis is running. Exiting.")
        sys.exit(1)

    # Start health check thread
    health_thread = threading.Thread(target=health_check_worker)
    health_thread.daemon = True
    health_thread.start()

    # Start CLI interface
    from crawler_cli import main as cli_main
    cli_main()

if __name__ == "__main__":
    main()