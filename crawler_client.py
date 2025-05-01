#!/usr/bin/env python3
# filepath: crawler_client.py

"""
Simplified client interface for the distributed web crawler.
Provides essential commands to manage crawling with minimal monitoring.
"""

import os
import sys
import time
import subprocess
import requests
import json
from tabulate import tabulate
from datetime import datetime

# Import crawler components
from distributed_config import CRAWLER_IP, INDEXER_IP, MASTER_IP
from aws_config import ensure_aws_clients, S3_BUCKET_NAME, S3_OUTPUT_PREFIX
from crawler_config import CrawlerConfig
from coordinator import start_crawl

# ANSI color codes
class Colors:
    GREEN = '\033[92m'
    CYAN = '\033[96m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'

def print_banner():
    """Print simple banner"""
    print(f"\n{Colors.BOLD}=== DISTRIBUTED WEB CRAWLER ===\n{Colors.ENDC}")

def check_aws_credentials():
    """Check if AWS credentials are configured properly"""
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
    missing = [var for var in required_vars if not os.environ.get(var)]

    if missing:
        print(f"{Colors.RED}Error: Missing AWS environment variables: {', '.join(missing)}{Colors.ENDC}")
        return False

    return True

def ssh_execute(node_ip, command, description=None):
    """Execute a command on a remote node using SSH"""
    ssh_key_path = os.environ.get('AWS_SSH_KEY_PATH', '~/.ssh/aws-key.pem')
    ssh_user = os.environ.get('AWS_SSH_USER', 'ec2-user')
    ssh_options = "-o StrictHostKeyChecking=no -o ConnectTimeout=5"

    if description:
        print(f"{Colors.CYAN}● {description}{Colors.ENDC}")

    try:
        ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{node_ip} '{command}'"
        result = subprocess.run(ssh_cmd, shell=True, timeout=30,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode == 0:
            return True
        else:
            print(f"{Colors.RED}✗ Command failed: {result.stderr.decode().strip()}{Colors.ENDC}")
            return False
    except Exception as e:
        print(f"{Colors.RED}✗ SSH error: {e}{Colors.ENDC}")
        return False

def check_node_status():
    """Check status of all nodes"""
    results = {
        "master": {"status": "DOWN", "message": ""},
        "crawler": {"status": "DOWN", "message": ""},
        "indexer": {"status": "DOWN", "message": ""}
    }

    # Check each node
    nodes = [
        ("master", MASTER_IP),
        ("crawler", CRAWLER_IP),
        ("indexer", INDEXER_IP)
    ]

    print(f"\n{Colors.BOLD}Checking node status...{Colors.ENDC}")
    for name, ip in nodes:
        try:
            response = requests.get(f"http://{ip}:8080/health", timeout=3)
            if response.status_code == 200:
                results[name] = {"status": "RUNNING", "message": ""}
                print(f"{Colors.GREEN}✓ {name.capitalize()}: RUNNING{Colors.ENDC}")
            else:
                print(f"{Colors.RED}✗ {name.capitalize()}: ERROR (HTTP {response.status_code}){Colors.ENDC}")
        except Exception as e:
            print(f"{Colors.RED}✗ {name.capitalize()}: DOWN ({str(e).split('(')[0].strip()}){Colors.ENDC}")

    return results

def get_crawl_stats():
    """Get statistics about crawling progress"""
    ensure_aws_clients()
    from aws_config import s3_client, sqs_client, SQS_CRAWLER_QUEUE_NAME, SQS_INDEXER_QUEUE_NAME

    stats = {
        "crawled_pages": 0,
        "crawl_queue": 0,
        "index_queue": 0,
        "latest_url": "None"
    }

    try:
        # Get crawled pages count from S3
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_OUTPUT_PREFIX
        )

        if 'Contents' in response:
            stats["crawled_pages"] = sum(1 for item in response['Contents'] if item['Key'].endswith('.json'))

            # Get most recent URL
            sorted_files = sorted(
                [item for item in response['Contents'] if item['Key'].endswith('.json')],
                key=lambda x: x['LastModified'],
                reverse=True
            )

            if sorted_files:
                try:
                    obj = s3_client.get_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=sorted_files[0]['Key']
                    )
                    content = json.loads(obj['Body'].read().decode('utf-8'))
                    stats["latest_url"] = content.get('url', 'Unknown URL')
                except:
                    pass

        # Get queue stats (SQS)
        queue_url = sqs_client.get_queue_url(QueueName=SQS_CRAWLER_QUEUE_NAME)['QueueUrl']
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        stats["crawl_queue"] = (
            int(attrs['Attributes']['ApproximateNumberOfMessages']) +
            int(attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        )

        queue_url = sqs_client.get_queue_url(QueueName=SQS_INDEXER_QUEUE_NAME)['QueueUrl']
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        stats["index_queue"] = (
            int(attrs['Attributes']['ApproximateNumberOfMessages']) +
            int(attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        )
    except Exception as e:
        print(f"{Colors.RED}Error getting stats: {e}{Colors.ENDC}")

    return stats

def start_all_components():
    """Start all crawler system components"""
    print(f"\n{Colors.BOLD}Starting crawler components...{Colors.ENDC}")

    # Check node status
    status = check_node_status()
    all_running = all(node["status"] == "RUNNING" for node in status.values())

    if all_running:
        print(f"{Colors.GREEN}All components already running!{Colors.ENDC}")
        return True

    if not check_aws_credentials():
        return False

    # Start each node that's not running
    ssh_key_path = os.environ.get('AWS_SSH_KEY_PATH', '~/.ssh/aws-key.pem')
    ssh_user = os.environ.get('AWS_SSH_USER', 'ec2-user')
    ssh_options = "-o StrictHostKeyChecking=no -o ConnectTimeout=5"

    nodes_to_start = []

    if status["master"]["status"] != "RUNNING":
        nodes_to_start.append(("master", MASTER_IP, "run_master.py"))

    if status["crawler"]["status"] != "RUNNING":
        nodes_to_start.append(("crawler", CRAWLER_IP, "run_crawler.py"))

    if status["indexer"]["status"] != "RUNNING":
        nodes_to_start.append(("indexer", INDEXER_IP, "run_indexer.py"))

    for node_name, ip, script in nodes_to_start:
        print(f"Starting {node_name} node on {ip}...")
        ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{ip} 'cd ~/web_crawler && nohup python3 {script} > {node_name}.out 2>&1 &'"
        os.system(ssh_cmd)

    if nodes_to_start:
        print(f"{Colors.CYAN}Waiting for components to initialize (15s)...{Colors.ENDC}")
        time.sleep(15)

        # Check status again
        status = check_node_status()
        all_running = all(node["status"] == "RUNNING" for node in status.values())

        if not all_running:
            print(f"{Colors.YELLOW}Warning: Some components failed to start properly.{Colors.ENDC}")

    return True

def monitor_crawl_progress(task_ids):
    """Simple monitor for crawl progress"""
    print(f"\n{Colors.BOLD}CRAWL MONITORING{Colors.ENDC}")
    print("=" * 60)
    print(f"Started crawl with {len(task_ids)} seed tasks at {datetime.now().strftime('%H:%M:%S')}")
    print(f"Press Ctrl+C to stop monitoring (crawl will continue in background)\n")

    # Track stats
    initial_stats = get_crawl_stats()
    last_update = time.time()
    stable_count = initial_stats["crawled_pages"]
    stable_since = time.time()
    no_change_duration = 0
    max_runtime = 1800  # 30 minutes

    try:
        while True:
            current_time = time.time()
            elapsed_time = current_time - last_update

            # Update stats every 5 seconds
            if elapsed_time >= 5:
                last_update = current_time
                stats = get_crawl_stats()

                # Check if complete
                if stats["crawl_queue"] == 0 and stats["index_queue"] == 0 and stats["crawled_pages"] > 0:
                    # Double check - wait 10 more seconds to be sure
                    time.sleep(10)
                    stats = get_crawl_stats()
                    if stats["crawl_queue"] == 0 and stats["index_queue"] == 0:
                        print("\n" + "=" * 60)
                        print(f"{Colors.GREEN}{Colors.BOLD}CRAWL COMPLETE!{Colors.ENDC}")
                        print(f"Pages crawled: {stats['crawled_pages']}")
                        print(f"Final timestamp: {datetime.now().strftime('%H:%M:%S')}")
                        return True

                # Check if count has stabilized
                if stats["crawled_pages"] != stable_count:
                    stable_count = stats["crawled_pages"]
                    stable_since = time.time()
                    no_change_duration = 0
                else:
                    no_change_duration = time.time() - stable_since

                # Print status line
                print(f"\r[{datetime.now().strftime('%H:%M:%S')}] " +
                      f"Pages: {stats['crawled_pages']} | " +
                      f"Queue: {stats['crawl_queue']} | " +
                      f"Index: {stats['index_queue']} | " +
                      f"Stable: {int(no_change_duration)}s | " +
                      f"Latest: {stats['latest_url'][:40] + '...' if len(stats['latest_url']) > 40 else stats['latest_url']}",
                      end="", flush=True)

                # Check if stable for too long
                if no_change_duration > 120 and stats["crawled_pages"] > 0 and stats["crawl_queue"] == 0 and stats["index_queue"] == 0:
                    print("\n" + "=" * 60)
                    print(f"{Colors.GREEN}{Colors.BOLD}CRAWL COMPLETE!{Colors.ENDC}")
                    print(f"Pages crawled: {stats['crawled_pages']}")
                    print(f"Final timestamp: {datetime.now().strftime('%H:%M:%S')}")
                    return True

                # Check if running too long
                if time.time() - stable_since > max_runtime:
                    print("\n" + "=" * 60)
                    print(f"{Colors.YELLOW}{Colors.BOLD}CRAWL TIMEOUT{Colors.ENDC}")
                    print(f"Crawl exceeded maximum runtime of {max_runtime/60} minutes")
                    print(f"Pages crawled so far: {stats['crawled_pages']}")
                    return False

            time.sleep(1)

    except KeyboardInterrupt:
        stats = get_crawl_stats()
        print("\n" + "=" * 60)
        print(f"{Colors.YELLOW}Monitoring stopped by user{Colors.ENDC}")
        print(f"Pages crawled so far: {stats['crawled_pages']}")
        print(f"Crawler will continue running in the background")
        return None

def start_crawl_process():
    """Start and monitor a crawl process"""
    print_banner()

    # Check and start components if needed
    if not start_all_components():
        print(f"{Colors.RED}Failed to start required components.{Colors.ENDC}")
        return False

    # Show configuration
    config = CrawlerConfig().get_config()
    print(f"\n{Colors.BOLD}CRAWL CONFIGURATION{Colors.ENDC}")
    print("=" * 60)
    print(f"Seed URLs: {', '.join(config['seed_urls'])}")
    print(f"Max depth: {config['max_depth']}")
    print(f"Workers: {config['num_crawlers']} crawlers, {config['num_indexers']} indexers")
    print(f"Request delay: {config['request_delay']}s, Timeout: {config['timeout']}s")

    # Confirm start
    start = input(f"\n{Colors.BOLD}Start crawling with these settings? (y/n): {Colors.ENDC}")
    if start.lower() != "y":
        print("Crawl cancelled.")
        return False

    # Start the crawl
    print(f"\n{Colors.CYAN}Starting crawler...{Colors.ENDC}")
    try:
        task_ids = start_crawl()
        print(f"{Colors.GREEN}Submitted {len(task_ids)} seed tasks.{Colors.ENDC}")

        # Monitor progress
        result = monitor_crawl_progress(task_ids)
        if result is True:
            print(f"\n{Colors.GREEN}Crawl process completed successfully!{Colors.ENDC}")
        elif result is False:
            print(f"\n{Colors.YELLOW}Crawl process timed out or had issues.{Colors.ENDC}")
        # If None, user interrupted monitoring

    except Exception as e:
        print(f"{Colors.RED}Error starting crawl: {e}{Colors.ENDC}")
        return False

    return True

def main():
    """Main entry point"""
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--check":
            # Just check status
            print_banner()
            check_node_status()
            stats = get_crawl_stats()
            print(f"\nPages crawled: {stats['crawled_pages']}")
            print(f"Crawler queue: {stats['crawl_queue']}")
            print(f"Indexer queue: {stats['index_queue']}")
        else:
            # Start and monitor crawl
            start_crawl_process()

    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Process interrupted by user.{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.RED}Error: {e}{Colors.ENDC}")

if __name__ == "__main__":
    main()