#!/usr/bin/env python3
# filepath: crawler_client.py

"""
Unified client interface for the distributed web crawler.
Provides a single command to manage crawling, indexing, and searching operations.
"""

import os
import sys
import time
import argparse
import threading
import subprocess
import requests
import json
import textwrap
from datetime import datetime
from tabulate import tabulate

# Import crawler components
from distributed_config import CRAWLER_IP, INDEXER_IP, MASTER_IP
from aws_config import ensure_aws_clients, S3_BUCKET_NAME, S3_OUTPUT_PREFIX
from crawler_config import CrawlerConfig
from search import interactive_search, search_content, print_result, print_header
from coordinator import start_crawl

# ANSI color codes
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_banner():
    """Print the application banner"""
    clear_screen()
    banner = f"""
{Colors.BOLD}{Colors.BLUE}╔══════════════════════════════════════════════════════════╗
║                                                          ║
║               DISTRIBUTED WEB CRAWLER CLIENT             ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝{Colors.ENDC}

{Colors.CYAN}Running on AWS infrastructure with OpenSearch, DynamoDB, and S3{Colors.ENDC}
"""
    print(banner)

def check_aws_credentials():
    """Check if AWS credentials are configured properly"""
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
    missing = [var for var in required_vars if not os.environ.get(var)]

    if missing:
        print(f"{Colors.RED}Error: Missing AWS environment variables: {', '.join(missing)}{Colors.ENDC}")
        print("\nPlease set these variables before running the client:")
        for var in missing:
            print(f"  export {var}=your_{var.lower()}")
        return False

    return True

def check_node_status():
    """Check status of all crawler nodes"""
    results = {
        "master": {"status": "DOWN", "message": ""},
        "crawler": {"status": "DOWN", "message": ""},
        "indexer": {"status": "DOWN", "message": ""}
    }

    # Check Master
    try:
        response = requests.get(f"http://{MASTER_IP}:8080/health", timeout=3)
        if response.status_code == 200:
            results["master"] = {"status": "RUNNING", "message": ""}
        else:
            results["master"] = {"status": "ERROR", "message": f"HTTP {response.status_code}"}
    except Exception as e:
        results["master"]["message"] = str(e)

    # Check Crawler
    try:
        response = requests.get(f"http://{CRAWLER_IP}:8080/health", timeout=3)
        if response.status_code == 200:
            results["crawler"] = {"status": "RUNNING", "message": ""}
        else:
            results["crawler"] = {"status": "ERROR", "message": f"HTTP {response.status_code}"}
    except Exception as e:
        results["crawler"]["message"] = str(e)

    # Check Indexer
    try:
        response = requests.get(f"http://{INDEXER_IP}:8080/health", timeout=3)
        if response.status_code == 200:
            results["indexer"] = {"status": "RUNNING", "message": ""}
        else:
            results["indexer"] = {"status": "ERROR", "message": f"HTTP {response.status_code}"}
    except Exception as e:
        results["indexer"]["message"] = str(e)

    return results

def get_crawl_stats():
    """Get statistics about crawling progress"""
    # Initialize AWS clients
    ensure_aws_clients()
    from aws_config import s3_client, sqs_client, SQS_CRAWLER_QUEUE_NAME, SQS_INDEXER_QUEUE_NAME

    stats = {
        "crawled_pages": 0,
        "crawl_queue": 0,
        "index_queue": 0,
        "latest_crawls": []
    }

    # Get crawled pages count from S3
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_OUTPUT_PREFIX
        )
        if 'Contents' in response:
            stats["crawled_pages"] = sum(1 for item in response['Contents'] if item['Key'].endswith('.json'))

            # Get latest crawls
            sorted_files = sorted(
                [item for item in response['Contents'] if item['Key'].endswith('.json')],
                key=lambda x: x['LastModified'],
                reverse=True
            )[:5]  # Get 5 most recent

            for item in sorted_files:
                try:
                    obj = s3_client.get_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=item['Key']
                    )
                    content = json.loads(obj['Body'].read().decode('utf-8'))
                    stats["latest_crawls"].append({
                        "url": content.get('url', 'Unknown URL'),
                        "title": content.get('title', 'Unknown Title'),
                        "timestamp": datetime.fromtimestamp(
                            content.get('crawl_timestamp', 0)
                        ).strftime('%Y-%m-%d %H:%M:%S')
                    })
                except Exception:
                    pass
    except Exception as e:
        print(f"{Colors.WARNING}Error getting S3 stats: {e}{Colors.ENDC}")

    # Get queue stats
    try:
        # Crawler queue
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
        except Exception as e:
            print(f"{Colors.WARNING}Error getting crawler queue stats: {e}{Colors.ENDC}")

        # Indexer queue
        try:
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
            print(f"{Colors.WARNING}Error getting indexer queue stats: {e}{Colors.ENDC}")
    except Exception as e:
        print(f"{Colors.WARNING}Error getting queue stats: {e}{Colors.ENDC}")

    return stats

def start_all_components():
    """Start all components of the crawler system"""
    print(f"\n{Colors.CYAN}Starting all crawler components...{Colors.ENDC}")

    # Check if components are already running
    status = check_node_status()
    already_running = any(node["status"] == "RUNNING" for node in status.values())

    if already_running:
        print(f"\n{Colors.WARNING}Some components are already running:{Colors.ENDC}")
        for node, info in status.items():
            status_color = Colors.GREEN if info["status"] == "RUNNING" else Colors.RED
            print(f"  - {node.capitalize()}: {status_color}{info['status']}{Colors.ENDC}")

        choice = input(f"\n{Colors.BOLD}Start missing components? (y/n): {Colors.ENDC}").lower()
        if choice != 'y':
            return

    # First check AWS credentials
    if not check_aws_credentials():
        return

    print(f"\n{Colors.CYAN}Launching system components...{Colors.ENDC}")

    # 1. Start master (if needed)
    if status["master"]["status"] != "RUNNING":
        print(f"Starting master node on {MASTER_IP}...")
        # In a production system, you would use SSH to start the process on the remote machine
        # For now, we'll simulate the start with a local command
        subprocess.Popen(["python3", "run_master.py"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        time.sleep(2)

    # 2. Start crawler (if needed)
    if status["crawler"]["status"] != "RUNNING":
        print(f"Starting crawler node on {CRAWLER_IP}...")
        # In a production system, use SSH to start on the remote machine
        subprocess.Popen(["python3", "run_crawler.py"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        time.sleep(2)

    # 3. Start indexer (if needed)
    if status["indexer"]["status"] != "RUNNING":
        print(f"Starting indexer node on {INDEXER_IP}...")
        # In a production system, use SSH to start on the remote machine
        subprocess.Popen(["python3", "run_indexer.py"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        time.sleep(2)

    # Wait for startup and check status
    print(f"\n{Colors.CYAN}Waiting for components to initialize...{Colors.ENDC}")
    time.sleep(5)

    status = check_node_status()
    all_running = all(node["status"] == "RUNNING" for node in status.values())

    if all_running:
        print(f"\n{Colors.GREEN}✓ All components started successfully!{Colors.ENDC}")
    else:
        print(f"\n{Colors.WARNING}Some components failed to start:{Colors.ENDC}")
        for node, info in status.items():
            status_color = Colors.GREEN if info["status"] == "RUNNING" else Colors.RED
            print(f"  - {node.capitalize()}: {status_color}{info['status']}{Colors.ENDC}")
            if info["message"]:
                print(f"    Error: {info['message']}")

def show_dashboard():
    """Display the crawler system dashboard"""
    while True:
        clear_screen()
        print_banner()

        # Get current status
        status = check_node_status()
        stats = get_crawl_stats()

        # Display components status
        print(f"{Colors.BOLD}SYSTEM COMPONENTS STATUS{Colors.ENDC}")
        print("=" * 60)

        status_table = []
        for node, info in status.items():
            status_str = f"{Colors.GREEN}✓ RUNNING{Colors.ENDC}" if info["status"] == "RUNNING" else f"{Colors.RED}✗ {info['status']}{Colors.ENDC}"
            status_table.append([node.capitalize(), status_str, info["message"]])

        print(tabulate(status_table, headers=["Component", "Status", "Message"], tablefmt="simple"))

        # Display crawling statistics
        print(f"\n{Colors.BOLD}CRAWLING STATISTICS{Colors.ENDC}")
        print("=" * 60)
        print(f"Crawled Pages: {Colors.GREEN}{stats['crawled_pages']}{Colors.ENDC}")
        print(f"Crawler Queue: {Colors.CYAN}{stats['crawl_queue']}{Colors.ENDC} tasks")
        print(f"Indexer Queue: {Colors.CYAN}{stats['index_queue']}{Colors.ENDC} tasks")

        # Display latest crawls
        if stats["latest_crawls"]:
            print(f"\n{Colors.BOLD}RECENTLY CRAWLED PAGES{Colors.ENDC}")
            print("=" * 60)
            for i, page in enumerate(stats["latest_crawls"], 1):
                print(f"{i}. {Colors.GREEN}{page['title']}{Colors.ENDC}")
                print(f"   URL: {Colors.UNDERLINE}{page['url']}{Colors.ENDC}")
                print(f"   Time: {page['timestamp']}")

        # Display options
        print(f"\n{Colors.BOLD}COMMANDS{Colors.ENDC}")
        print("=" * 60)
        print(f"1. {Colors.CYAN}Start new crawl{Colors.ENDC}")
        print(f"2. {Colors.CYAN}Search crawled content{Colors.ENDC}")
        print(f"3. {Colors.CYAN}Purge crawled data{Colors.ENDC}")
        print(f"4. {Colors.CYAN}View/modify configuration{Colors.ENDC}")
        print(f"r. {Colors.CYAN}Refresh dashboard{Colors.ENDC}")
        print(f"q. {Colors.CYAN}Quit{Colors.ENDC}")

        choice = input(f"\n{Colors.BOLD}Enter command: {Colors.ENDC}")

        if choice == '1':
            start_new_crawl()
        elif choice == '2':
            search_interface()
        elif choice == '3':
            purge_data()
        elif choice == '4':
            modify_config()
        elif choice == 'r':
            continue  # Refresh by continuing the loop
        elif choice.lower() == 'q':
            break
        else:
            input(f"{Colors.WARNING}Invalid choice. Press Enter to continue...{Colors.ENDC}")

def start_new_crawl():
    """Start a new crawling operation"""
    clear_screen()
    print_banner()
    print(f"{Colors.BOLD}START NEW CRAWL{Colors.ENDC}")
    print("=" * 60)

    # Load current configuration
    config = CrawlerConfig().get_config()

    # Show current seed URLs
    print(f"{Colors.BOLD}Current seed URLs:{Colors.ENDC}")
    for i, url in enumerate(config['seed_urls'], 1):
        print(f"{i}. {url}")

    # Options for starting crawl
    print(f"\n{Colors.BOLD}Options:{Colors.ENDC}")
    print(f"1. Start crawl with current seed URLs")
    print(f"2. Enter new seed URL")
    print(f"3. Return to dashboard")

    choice = input(f"\n{Colors.BOLD}Enter choice: {Colors.ENDC}")

    if choice == '1':
        if not config['seed_urls']:
            print(f"\n{Colors.RED}Error: No seed URLs configured.{Colors.ENDC}")
            input("Press Enter to continue...")
            return

        # Confirm crawl parameters
        print(f"\n{Colors.BOLD}Crawl parameters:{Colors.ENDC}")
        print(f"Seed URLs: {', '.join(config['seed_urls'])}")
        print(f"Max depth: {config['max_depth']}")
        print(f"Num crawlers: {config['num_crawlers']}")
        print(f"Num indexers: {config['num_indexers']}")

        confirm = input(f"\n{Colors.BOLD}Start crawl with these parameters? (y/n): {Colors.ENDC}")
        if confirm.lower() != 'y':
            return

        # Start the crawl
        print(f"\n{Colors.CYAN}Starting crawler...{Colors.ENDC}")
        task_ids = start_crawl()
        print(f"\n{Colors.GREEN}Crawl started with {len(task_ids)} seed tasks.{Colors.ENDC}")
        input("Press Enter to return to dashboard...")

    elif choice == '2':
        new_url = input(f"\n{Colors.BOLD}Enter new seed URL: {Colors.ENDC}")
        if not new_url:
            print(f"{Colors.RED}No URL entered.{Colors.ENDC}")
            input("Press Enter to continue...")
            return

        # Update config with new URL
        config['seed_urls'] = [new_url]
        CrawlerConfig().config = config
        CrawlerConfig().save_config()

        print(f"\n{Colors.GREEN}Seed URL updated: {new_url}{Colors.ENDC}")

        # Ask to start crawl
        confirm = input(f"\n{Colors.BOLD}Start crawl with this URL now? (y/n): {Colors.ENDC}")
        if confirm.lower() == 'y':
            task_ids = start_crawl()
            print(f"\n{Colors.GREEN}Crawl started with {len(task_ids)} seed tasks.{Colors.ENDC}")

        input("Press Enter to return to dashboard...")

def search_interface():
    """Interface for searching crawled content"""
    interactive_search()  # Use the existing interactive search

def purge_data():
    """Purge all crawled data"""
    clear_screen()
    print_banner()
    print(f"{Colors.BOLD}{Colors.RED}PURGE ALL CRAWLED DATA{Colors.ENDC}")
    print("=" * 60)
    print(f"{Colors.WARNING}Warning: This will permanently delete all crawled data!{Colors.ENDC}")
    print("This includes:")
    print("  - All content stored in S3 bucket")
    print("  - All task data in DynamoDB table")
    print("  - All indexed content in OpenSearch")

    confirm = input(f"\n{Colors.BOLD}Type 'PURGE' to confirm deletion: {Colors.ENDC}")
    if confirm != 'PURGE':
        print("Purge operation cancelled.")
        input("Press Enter to return to dashboard...")
        return

    # Run the purge operation
    print(f"\n{Colors.CYAN}Purging all data...{Colors.ENDC}")
    from crawler_cli import purge_data

    # Create args object with force=True
    class Args:
        force = True

    purge_data(Args())
    input(f"\n{Colors.BOLD}Press Enter to return to dashboard...{Colors.ENDC}")

def modify_config():
    """View and modify crawler configuration"""
    clear_screen()
    print_banner()
    print(f"{Colors.BOLD}CRAWLER CONFIGURATION{Colors.ENDC}")
    print("=" * 60)

    config = CrawlerConfig().get_config()

    # Display current config
    print(f"{Colors.BOLD}Current Configuration:{Colors.ENDC}")
    print(f"1. Seed URLs: {', '.join(config['seed_urls'])}")
    print(f"2. Max Depth: {config['max_depth']}")
    print(f"3. Number of Crawlers: {config['num_crawlers']}")
    print(f"4. Number of Indexers: {config['num_indexers']}")
    print(f"5. Request Delay: {config['request_delay']} seconds")
    print(f"6. Request Timeout: {config['timeout']} seconds")
    print(f"7. OpenSearch Index Name: {config.get('elasticsearch_index', 'webcrawler')}")
    print(f"8. Restricted Domains: {', '.join(config['restricted_domains'])}")

    print(f"\n{Colors.BOLD}Options:{Colors.ENDC}")
    print(f"Enter the number to modify that setting")
    print(f"s. Save configuration")
    print(f"r. Return to dashboard")

    choice = input(f"\n{Colors.BOLD}Enter choice: {Colors.ENDC}")

    if choice == '1':
        urls = input(f"Enter seed URLs (comma-separated): ")
        if urls:
            config['seed_urls'] = [u.strip() for u in urls.split(',')]
    elif choice == '2':
        try:
            depth = int(input(f"Enter max depth: "))
            config['max_depth'] = depth
        except ValueError:
            print(f"{Colors.RED}Invalid value. Must be an integer.{Colors.ENDC}")
    elif choice == '3':
        try:
            num = int(input(f"Enter number of crawlers: "))
            config['num_crawlers'] = num
        except ValueError:
            print(f"{Colors.RED}Invalid value. Must be an integer.{Colors.ENDC}")
    elif choice == '4':
        try:
            num = int(input(f"Enter number of indexers: "))
            config['num_indexers'] = num
        except ValueError:
            print(f"{Colors.RED}Invalid value. Must be an integer.{Colors.ENDC}")
    elif choice == '5':
        try:
            delay = float(input(f"Enter request delay (seconds): "))
            config['request_delay'] = delay
        except ValueError:
            print(f"{Colors.RED}Invalid value. Must be a number.{Colors.ENDC}")
    elif choice == '6':
        try:
            timeout = int(input(f"Enter request timeout (seconds): "))
            config['timeout'] = timeout
        except ValueError:
            print(f"{Colors.RED}Invalid value. Must be an integer.{Colors.ENDC}")
    elif choice == '7':
        index = input(f"Enter OpenSearch index name: ")
        if index:
            config['elasticsearch_index'] = index
    elif choice == '8':
        domains = input(f"Enter restricted domains (comma-separated): ")
        if domains:
            config['restricted_domains'] = [d.strip() for d in domains.split(',')]
    elif choice.lower() == 's':
        # Save configuration
        CrawlerConfig().config = config
        if CrawlerConfig().save_config():
            print(f"\n{Colors.GREEN}Configuration saved successfully.{Colors.ENDC}")
        else:
            print(f"\n{Colors.RED}Error saving configuration.{Colors.ENDC}")
        input("Press Enter to continue...")
        return
    elif choice.lower() == 'r':
        return
    else:
        print(f"\n{Colors.RED}Invalid choice.{Colors.ENDC}")
        input("Press Enter to continue...")
        return

    # Save automatically after modifying a setting
    CrawlerConfig().config = config
    if CrawlerConfig().save_config():
        print(f"\n{Colors.GREEN}Configuration updated and saved.{Colors.ENDC}")
    else:
        print(f"\n{Colors.RED}Configuration updated but not saved.{Colors.ENDC}")

    input("Press Enter to continue...")
    modify_config()  # Return to config menu

def main():
    """Main entry point for the crawler client"""
    parser = argparse.ArgumentParser(description="Distributed Web Crawler Client")
    parser.add_argument("--start-all", action="store_true", help="Start all components")
    parser.add_argument("--search", nargs='?', const=True, help="Search crawled content")
    parser.add_argument("--dashboard", action="store_true", help="Show the crawler dashboard")

    args = parser.parse_args()

    # If no args provided, show dashboard by default
    if len(sys.argv) == 1:
        args.dashboard = True

    try:
        if args.start_all:
            print_banner()
            start_all_components()

            if not args.dashboard and not args.search:
                print(f"\n{Colors.CYAN}Hint: Run with --dashboard to monitor the crawler{Colors.ENDC}")

        if args.search:
            if args.search is True:
                # Interactive search mode
                interactive_search()
            else:
                # Direct search query
                results = search_content(args.search)
                print_header(f"SEARCH RESULTS FOR: {args.search}")

                if not results:
                    print(f"No results found for '{args.search}'")
                else:
                    for i, result in enumerate(results, 1):
                        print_result(i, result)

        if args.dashboard:
            show_dashboard()

    except KeyboardInterrupt:
        print(f"\n{Colors.CYAN}Exiting crawler client...{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.RED}Error: {e}{Colors.ENDC}")

if __name__ == "__main__":
    main()