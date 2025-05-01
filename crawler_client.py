#!/usr/bin/env python3
# filepath: crawler_client.py

"""
Simplified interface for the distributed web crawler.
Provides streamlined commands for starting, monitoring, and searching.
"""

import os
import sys
import time
import argparse
import subprocess
import requests
import json
from datetime import datetime

# Import crawler components
from distributed_config import CRAWLER_IP, INDEXER_IP, MASTER_IP
from aws_config import ensure_aws_clients, S3_BUCKET_NAME, S3_OUTPUT_PREFIX
from crawler_config import CrawlerConfig
from search import search_content
from coordinator import start_crawl

# ANSI color codes
class Colors:
    GREEN = '\033[92m'
    CYAN = '\033[96m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'

def print_banner():
    """Print simple banner"""
    banner = f"\n{Colors.BOLD}=== DISTRIBUTED WEB CRAWLER ===\n{Colors.ENDC}"
    print(banner)

def check_aws_credentials():
    """Check if AWS credentials are configured properly"""
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
    missing = [var for var in required_vars if not os.environ.get(var)]

    if missing:
        print(f"{Colors.RED}Error: Missing AWS environment variables: {', '.join(missing)}{Colors.ENDC}")
        return False

    return True

def ssh_command(node_ip, command):
    """Execute SSH command on remote node"""
    ssh_key_path = os.environ.get('AWS_SSH_KEY_PATH', '~/.ssh/aws-key.pem')
    ssh_user = os.environ.get('AWS_SSH_USER', 'ec2-user')
    ssh_options = "-o StrictHostKeyChecking=no -o ConnectTimeout=5"

    ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{node_ip} '{command}'"
    return subprocess.run(ssh_cmd, shell=True, timeout=30,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def check_node_status():
    """Check status of crawler components"""
    nodes = {
        "Master": MASTER_IP,
        "Crawler": CRAWLER_IP,
        "Indexer": INDEXER_IP
    }

    status = {}

    for name, ip in nodes.items():
        try:
            response = requests.get(f"http://{ip}:8080/health", timeout=2)
            status[name] = response.status_code == 200
        except:
            status[name] = False

    return status

def start_nodes():
    """Start all crawler nodes"""
    print(f"{Colors.CYAN}Starting all crawler components...{Colors.ENDC}")

    # Check status first
    current_status = check_node_status()
    nodes_to_start = []

    for name, running in current_status.items():
        if not running:
            nodes_to_start.append(name)

    if not nodes_to_start:
        print(f"{Colors.GREEN}All nodes are already running!{Colors.ENDC}")
        return True

    print(f"{Colors.YELLOW}Starting: {', '.join(nodes_to_start)}{Colors.ENDC}")

    # Check AWS creds
    if not check_aws_credentials():
        return False

    ssh_key_path = os.environ.get('AWS_SSH_KEY_PATH', '~/.ssh/aws-key.pem')
    ssh_user = os.environ.get('AWS_SSH_USER', 'ec2-user')
    ssh_options = "-o StrictHostKeyChecking=no -o ConnectTimeout=5"

    # Start each node as needed
    if "Master" in nodes_to_start:
        print(f"Starting Master node...")
        ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{MASTER_IP} 'cd ~/web_crawler && nohup python3 run_master.py > master.out 2>&1 &'"
        os.system(ssh_cmd)

    if "Crawler" in nodes_to_start:
        print(f"Starting Crawler node...")
        ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{CRAWLER_IP} 'cd ~/web_crawler && nohup python3 run_crawler.py > crawler.out 2>&1 &'"
        os.system(ssh_cmd)

    if "Indexer" in nodes_to_start:
        print(f"Starting Indexer node...")
        ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{INDEXER_IP} 'cd ~/web_crawler && nohup python3 run_indexer.py > indexer.out 2>&1 &'"
        os.system(ssh_cmd)

    # Wait for startup
    print(f"{Colors.CYAN}Waiting for components to initialize (15s)...{Colors.ENDC}")
    time.sleep(15)

    # Check status again
    new_status = check_node_status()
    all_running = all(new_status.values())

    if all_running:
        print(f"{Colors.GREEN}All components started successfully!{Colors.ENDC}")
    else:
        print(f"{Colors.YELLOW}Some components may have failed to start:{Colors.ENDC}")
        for name, running in new_status.items():
            status_str = f"{Colors.GREEN}RUNNING{Colors.ENDC}" if running else f"{Colors.RED}DOWN{Colors.ENDC}"
            print(f"  - {name}: {status_str}")

    return all_running

def get_stats():
    """Get current crawling stats"""
    ensure_aws_clients()
    from aws_config import s3_client, sqs_client, SQS_CRAWLER_QUEUE_NAME, SQS_INDEXER_QUEUE_NAME

    stats = {
        "crawled_pages": 0,
        "crawl_queue": 0,
        "index_queue": 0,
        "latest_url": "None"
    }

    try:
        # Get crawled pages from S3
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_OUTPUT_PREFIX
        )

        if 'Contents' in response:
            stats["crawled_pages"] = sum(1 for item in response['Contents'] if item['Key'].endswith('.json'))

            # Get most recent
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

        # Get queue stats
        try:
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
            print(f"Error getting queue stats: {e}")
    except Exception as e:
        print(f"Error getting stats: {e}")

    return stats

def monitor_crawl(task_ids, max_runtime=600, update_interval=5):
    """Monitor crawling progress with live updates"""
    print(f"{Colors.BOLD}CRAWL MONITORING{Colors.ENDC}")
    print(f"Started crawl at {datetime.now().strftime('%H:%M:%S')} with {len(task_ids)} seed URLs")
    print(f"Press Ctrl+C to stop monitoring (crawl will continue in background)\n")

    start_time = time.time()
    stable_count = 0
    stable_since = start_time
    stable_duration = 0
    last_count = 0

    try:
        while True:
            # Get current stats
            stats = get_stats()
            current_count = stats["crawled_pages"]
            now = time.time()

            # Check for stability (no changes in count)
            if current_count != last_count:
                stable_since = now
                stable_duration = 0
                last_count = current_count
            else:
                stable_duration = now - stable_since

            # Check if crawl is likely complete
            crawl_complete = (
                stats["crawl_queue"] == 0 and
                stats["index_queue"] == 0 and
                stable_duration > 10 and
                current_count > 0
            )

            # Check if running too long
            timeout = now - start_time > max_runtime

            # Display status
            runtime = int(now - start_time)
            status_line = (
                f"\r[{runtime}s] "
                f"Pages: {current_count} | "
                f"Queue: {stats['crawl_queue']} | "
                f"Index: {stats['index_queue']} | "
                f"Latest: {stats['latest_url'][:40] + '...' if len(stats['latest_url']) > 40 else stats['latest_url']}"
            )

            print(status_line, end="", flush=True)

            # Check for completion conditions
            if crawl_complete:
                print("\n\n" + "=" * 60)
                print(f"{Colors.GREEN}CRAWL COMPLETE!{Colors.ENDC}")
                print(f"Pages crawled: {current_count}")
                print(f"Runtime: {runtime} seconds")
                break

            if timeout:
                print("\n\n" + "=" * 60)
                print(f"{Colors.YELLOW}MAXIMUM RUNTIME REACHED.{Colors.ENDC}")
                print(f"Crawl may continue in the background.")
                print(f"Pages crawled so far: {current_count}")
                break

            time.sleep(update_interval)

    except KeyboardInterrupt:
        print("\n\n" + "=" * 60)
        print(f"{Colors.YELLOW}Monitoring stopped. Crawl continues in background.{Colors.ENDC}")
        stats = get_stats()
        print(f"Pages crawled so far: {stats['crawled_pages']}")

def launch_search():
    """Simple search interface"""
    print(f"\n{Colors.BOLD}SEARCH CRAWLED CONTENT{Colors.ENDC}")

    while True:
        query = input(f"\n{Colors.BOLD}Enter search query (or 'exit'): {Colors.ENDC}")

        if query.lower() == 'exit':
            break

        if not query:
            continue

        print(f"{Colors.CYAN}Searching for: {query}{Colors.ENDC}")
        results = search_content(query, show_progress=True)

        if not results:
            print(f"{Colors.YELLOW}No results found.{Colors.ENDC}")
            continue

        print(f"\n{Colors.GREEN}Found {len(results)} results:{Colors.ENDC}\n")

        for i, result in enumerate(results[:10], 1):  # Show top 10
            print(f"{i}. {Colors.BOLD}{result['title']}{Colors.ENDC} (Score: {result['score']:.2f})")
            print(f"   URL: {result['url']}")
            print(f"   {result['description'][:150]}..." if len(result['description']) > 150 else result['description'])
            if 'highlights' in result and result['highlights'].get('text_content'):
                print(f"   Highlights: {result['highlights']['text_content'][0]}...")
            print()

        if len(results) > 10:
            print(f"{Colors.YELLOW}Showing 10 of {len(results)} results.{Colors.ENDC}")

def one_command_crawl():
    """Start everything in one command with minimal interaction"""
    print_banner()

    print(f"{Colors.CYAN}Starting crawler system...{Colors.ENDC}")

    # 1. Start all nodes
    if not start_nodes():
        print(f"{Colors.RED}Failed to start some crawler components. Please check logs.{Colors.ENDC}")
        return False

    # 2. Display crawler config
    config = CrawlerConfig().get_config()
    print(f"\n{Colors.CYAN}Crawler configuration:{Colors.ENDC}")
    print(f"Seed URLs: {', '.join(config['seed_urls'])}")
    print(f"Max depth: {config['max_depth']}")
    print(f"Workers: {config['num_crawlers']} crawlers, {config['num_indexers']} indexers")

    # 3. Start crawling
    print(f"\n{Colors.CYAN}Starting crawl...{Colors.ENDC}")
    task_ids = start_crawl()
    print(f"{Colors.GREEN}Crawl started with {len(task_ids)} seed URLs{Colors.ENDC}")

    # 4. Monitor crawl
    monitor_crawl(task_ids)

    # 5. Prompt for search
    search_now = input(f"\n{Colors.BOLD}Would you like to search the crawled content? (y/n): {Colors.ENDC}")
    if search_now.lower() == 'y':
        launch_search()

    return True

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Simplified Web Crawler Client")
    parser.add_argument("command", nargs="?", choices=["run", "status", "search"],
                        default="run", help="Command to execute")
    parser.add_argument("--search-query", help="Search query (when using search)")

    args = parser.parse_args()

    try:
        if args.command == "run":
            one_command_crawl()
        elif args.command == "status":
            # Show current status
            status = check_node_status()
            print_banner()
            print(f"{Colors.BOLD}SYSTEM STATUS:{Colors.ENDC}")
            for name, running in status.items():
                status_str = f"{Colors.GREEN}RUNNING{Colors.ENDC}" if running else f"{Colors.RED}DOWN{Colors.ENDC}"
                print(f"- {name}: {status_str}")

            stats = get_stats()
            print(f"\n{Colors.BOLD}CRAWL STATISTICS:{Colors.ENDC}")
            print(f"Pages crawled: {stats['crawled_pages']}")
            print(f"Crawler queue: {stats['crawl_queue']}")
            print(f"Indexer queue: {stats['index_queue']}")
            if stats['latest_url'] != "None":
                print(f"Latest URL: {stats['latest_url']}")

        elif args.command == "search":
            if args.search_query:
                print_banner()
                print(f"{Colors.CYAN}Searching for: {args.search_query}{Colors.ENDC}")
                results = search_content(args.search_query, show_progress=True)

                if not results:
                    print(f"{Colors.YELLOW}No results found.{Colors.ENDC}")
                else:
                    print(f"\n{Colors.GREEN}Found {len(results)} results:{Colors.ENDC}\n")
                    for i, result in enumerate(results[:10], 1):
                        print(f"{i}. {Colors.BOLD}{result['title']}{Colors.ENDC}")
                        print(f"   URL: {result['url']}")
                        print(f"   {result['description'][:150]}..." if len(result['description']) > 150 else result['description'])
                        print()
            else:
                launch_search()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Operation cancelled.{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.RED}Error: {e}{Colors.ENDC}")

if __name__ == "__main__":
    main()