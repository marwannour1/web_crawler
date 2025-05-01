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

# ANSI cursor control codes
class Cursor:
    UP = '\033[A'
    DOWN = '\033[B'
    RIGHT = '\033[C'
    LEFT = '\033[D'
    SAVE = '\033[s'
    RESTORE = '\033[u'
    CLEAR_LINE = '\033[2K'
    CLEAR_SCREEN = '\033[2J'
    HOME = '\033[H'

    @staticmethod
    def move_to(row, col):
        return f"\033[{row};{col}H"

    @staticmethod
    def clear_to_end():
        return "\033[K"

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

def ssh_execute(node_ip, command, description=None, return_output=False):
    """Execute a command on a remote node using SSH"""
    # Set up SSH configuration
    ssh_key_path = os.environ.get('AWS_SSH_KEY_PATH', '~/.ssh/aws-key.pem')
    ssh_user = os.environ.get('AWS_SSH_USER', 'ec2-user')
    ssh_options = "-o StrictHostKeyChecking=no -o ConnectTimeout=5"

    if description:
        print(f"{Colors.CYAN}{description}{Colors.ENDC}")

    try:
        # Build full SSH command
        ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{node_ip} '{command}'"

        # Execute command and capture output
        result = subprocess.run(ssh_cmd, shell=True, timeout=30,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               universal_newlines=True)

        if result.returncode == 0:
            if description:
                print(f"{Colors.GREEN}Command succeeded on {node_ip}{Colors.ENDC}")

            if return_output:
                return True, result.stdout
            return True
        else:
            error_msg = result.stderr.strip() or "Unknown error"
            print(f"{Colors.RED}Command failed on {node_ip}: {error_msg}{Colors.ENDC}")

            if return_output:
                return False, result.stderr
            return False
    except Exception as e:
        print(f"{Colors.RED}SSH execution error for {node_ip}: {e}{Colors.ENDC}")

        if return_output:
            return False, str(e)
        return False

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
        pass  # Suppress errors for in-place updates

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
        except Exception:
            pass  # Suppress errors for in-place updates

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
        except Exception:
            pass  # Suppress errors for in-place updates
    except Exception:
        pass  # Suppress errors for in-place updates

    return stats

def prepare_nodes_for_crawl():
    """Prepare all nodes for a new crawling operation with custom commands"""
    print(f"\n{Colors.CYAN}Preparing nodes for crawl...{Colors.ENDC}")

    success = True

    # 1. Prepare master node
    master_commands = [
        "cd ~/web_crawler",
        "git pull || echo 'No git repository'",  # Update to latest code if using git
        "python3 run_master.py",  # Ensure master is ready
    ]

    master_result = ssh_execute(
        MASTER_IP,
        " && ".join(master_commands),
        f"Preparing master node ({MASTER_IP})..."
    )
    success = success and master_result

    # 2. Prepare crawler node
    crawler_commands = [
        "cd ~/web_crawler",
        "git pull || echo 'No git repository'",  # Update to latest code if using git
        "python3 run_crawler.py"
    ]

    crawler_result = ssh_execute(
        CRAWLER_IP,
        " && ".join(crawler_commands),
        f"Preparing crawler node ({CRAWLER_IP})..."
    )
    success = success and crawler_result

    # 3. Prepare indexer node
    indexer_commands = [
        "cd ~/web_crawler",
        "git pull || echo 'No git repository'",  # Update to latest code if using git
        "python3 run_indexer.py"
    ]

    indexer_result = ssh_execute(
        INDEXER_IP,
        " && ".join(indexer_commands),
        f"Preparing indexer node ({INDEXER_IP})..."
    )
    success = success and indexer_result

    return success

def start_all_components():
    """Start all components of the crawler system using SSH for remote execution"""
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

    # Set up SSH key path - update this to your EC2 key location
    ssh_key_path = os.environ.get('AWS_SSH_KEY_PATH', '~/.ssh/aws-key.pem')
    ssh_user = os.environ.get('AWS_SSH_USER', 'ec2-user')
    ssh_options = "-o StrictHostKeyChecking=no -o ConnectTimeout=5"

    # 1. Start master (if needed)
    if status["master"]["status"] != "RUNNING":
        print(f"Starting master node on {MASTER_IP}...")
        try:
            # Use SSH to start the process on the remote machine
            ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{MASTER_IP} 'cd ~/web_crawler && nohup python3 run_master.py > master.out 2>&1 &'"
            os.system(ssh_cmd)
            print(f"Master startup command sent to {MASTER_IP}")
        except Exception as e:
            print(f"{Colors.RED}Error starting master node: {e}{Colors.ENDC}")
        time.sleep(3)  # Give more time for startup

    # 2. Start crawler (if needed)
    if status["crawler"]["status"] != "RUNNING":
        print(f"Starting crawler node on {CRAWLER_IP}...")
        try:
            # Use SSH to start on the remote machine
            ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{CRAWLER_IP} 'cd ~/web_crawler && nohup python3 run_crawler.py > crawler.out 2>&1 &'"
            os.system(ssh_cmd)
            print(f"Crawler startup command sent to {CRAWLER_IP}")
        except Exception as e:
            print(f"{Colors.RED}Error starting crawler node: {e}{Colors.ENDC}")
        time.sleep(3)  # Give more time for startup

    # 3. Start indexer (if needed)
    if status["indexer"]["status"] != "RUNNING":
        print(f"Starting indexer node on {INDEXER_IP}...")
        try:
            # Use SSH to start on the remote machine
            ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{INDEXER_IP} 'cd ~/web_crawler && nohup python3 run_indexer.py > indexer.out 2>&1 &'"
            os.system(ssh_cmd)
            print(f"Indexer startup command sent to {INDEXER_IP}")
        except Exception as e:
            print(f"{Colors.RED}Error starting indexer node: {e}{Colors.ENDC}")
        time.sleep(3)  # Give more time for startup

    # Wait for startup and check status (longer wait time)
    print(f"\n{Colors.CYAN}Waiting for components to initialize...{Colors.ENDC}")
    time.sleep(15)  # Give nodes more time to start up

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

        print(f"\n{Colors.CYAN}You may need to manually start components on each server:{Colors.ENDC}")
        print(f"  Master: ssh {ssh_user}@{MASTER_IP} 'cd ~/web_crawler && python3 run_master.py'")
        print(f"  Crawler: ssh {ssh_user}@{CRAWLER_IP} 'cd ~/web_crawler && python3 run_crawler.py'")
        print(f"  Indexer: ssh {ssh_user}@{INDEXER_IP} 'cd ~/web_crawler && python3 run_indexer.py'")

def start_master_node():
    """Start just the master node"""
    print(f"\n{Colors.CYAN}Starting master node...{Colors.ENDC}")

    # Set up SSH configuration
    ssh_key_path = os.environ.get('AWS_SSH_KEY_PATH', '~/.ssh/aws-key.pem')
    ssh_user = os.environ.get('AWS_SSH_USER', 'ec2-user')
    ssh_options = "-o StrictHostKeyChecking=no -o ConnectTimeout=5"

    try:
        # Use subprocess instead of os.system for better error handling
        ssh_cmd = f"ssh {ssh_options} -i {ssh_key_path} {ssh_user}@{MASTER_IP} 'cd ~/web_crawler && nohup python3 run_master.py > master.out 2>&1 &'"
        result = subprocess.run(ssh_cmd, shell=True, timeout=10,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode == 0:
            print(f"{Colors.GREEN}Master node startup command sent successfully{Colors.ENDC}")
            return True
        else:
            print(f"{Colors.RED}Failed to start master node: {result.stderr.decode()}{Colors.ENDC}")
            return False
    except Exception as e:
        print(f"{Colors.RED}Error starting master node: {e}{Colors.ENDC}")
        return False

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

        # Check if nodes are running before starting crawl
        status = check_node_status()
        all_running = all(node["status"] == "RUNNING" for node in status.values())

        if not all_running:
            print(f"\n{Colors.WARNING}Not all required nodes are running:{Colors.ENDC}")
            for node, info in status.items():
                status_color = Colors.GREEN if info["status"] == "RUNNING" else Colors.RED
                print(f"  - {node.capitalize()}: {status_color}{info['status']}{Colors.ENDC}")

            start_nodes = input(f"\n{Colors.BOLD}Start missing nodes before crawling? (y/n): {Colors.ENDC}")
            if start_nodes.lower() == 'y':
                print(f"\n{Colors.CYAN}Starting required nodes...{Colors.ENDC}")
                start_all_components()
            else:
                print(f"\n{Colors.RED}Crawl can't be started without all nodes running.{Colors.ENDC}")
                input("Press Enter to return to dashboard...")
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
            # Check if nodes are running before starting crawl
            status = check_node_status()
            all_running = all(node["status"] == "RUNNING" for node in status.values())

            if not all_running:
                print(f"\n{Colors.WARNING}Not all required nodes are running:{Colors.ENDC}")
                for node, info in status.items():
                    status_color = Colors.GREEN if info["status"] == "RUNNING" else Colors.RED
                    print(f"  - {node.capitalize()}: {status_color}{info['status']}{Colors.ENDC}")

                start_nodes = input(f"\n{Colors.BOLD}Start missing nodes before crawling? (y/n): {Colors.ENDC}")
                if start_nodes.lower() == 'y':
                    print(f"\n{Colors.CYAN}Starting required nodes...{Colors.ENDC}")
                    start_all_components()
                else:
                    print(f"\n{Colors.RED}Crawl can't be started without all nodes running.{Colors.ENDC}")
                    input("Press Enter to return to dashboard...")
                    return

            # Start the crawl with new URL
            print(f"\n{Colors.CYAN}Starting crawler...{Colors.ENDC}")
            task_ids = start_crawl()
            print(f"\n{Colors.GREEN}Crawl started with {len(task_ids)} seed tasks.{Colors.ENDC}")

        input("Press Enter to return to dashboard...")

def show_dashboard():
    """Display the crawler system dashboard with dynamic stat updates"""
    # Initial setup
    refresh_interval = 2  # Update stats every 2 seconds
    auto_refresh = True

    # Stats markers for finding positions to update
    crawled_pages_marker = "CRAWLED_PAGES_MARKER"
    crawl_queue_marker = "CRAWL_QUEUE_MARKER"
    index_queue_marker = "INDEX_QUEUE_MARKER"

    # Initial stats
    prev_stats = {
        "crawled_pages": 0,
        "crawl_queue": 0,
        "index_queue": 0
    }

    try:
        # Display the static dashboard elements
        clear_screen()
        print_banner()

        # Get node status
        status = check_node_status()

        # Display components status
        print(f"{Colors.BOLD}SYSTEM COMPONENTS STATUS{Colors.ENDC}")
        print("=" * 60)

        status_table = []
        for node, info in status.items():
            status_str = f"{Colors.GREEN}✓ RUNNING{Colors.ENDC}" if info["status"] == "RUNNING" else f"{Colors.RED}✗ {info['status']}{Colors.ENDC}"
            status_table.append([node.capitalize(), status_str, info["message"]])

        print(tabulate(status_table, headers=["Component", "Status", "Message"], tablefmt="simple"))

        # Show option to start master if it's down
        if status["master"]["status"] != "RUNNING":
            print(f"\n{Colors.BOLD}[M] Start Master Node{Colors.ENDC}")

        # Display crawling statistics section headers
        print(f"\n{Colors.BOLD}CRAWLING STATISTICS{Colors.ENDC}")
        print("=" * 60)

        # Print the markers for the dynamic stats
        print(f"Crawled Pages: {Colors.GREEN}{crawled_pages_marker}{Colors.ENDC}")
        print(f"Crawler Queue: {Colors.CYAN}{crawl_queue_marker}{Colors.ENDC} tasks")
        print(f"Indexer Queue: {Colors.CYAN}{index_queue_marker}{Colors.ENDC} tasks")

        # Get positions of the markers
        output = sys.stdout.write

        # Get initial stats
        stats = get_crawl_stats()

        # Replace markers with initial values
        # Save cursor position for later updates
        sys.stdout.write(Cursor.SAVE)

        # Find the lines with markers and update them
        lines = []
        for line in sys.stdout.buffer.readline():
            lines.append(line.decode('utf-8').rstrip())

        crawled_line = next((i for i, line in enumerate(lines) if crawled_pages_marker in line), None)
        crawl_queue_line = next((i for i, line in enumerate(lines) if crawl_queue_marker in line), None)
        index_queue_line = next((i for i, line in enumerate(lines) if index_queue_marker in line), None)

        # Display options
        print(f"\n{Colors.BOLD}COMMANDS{Colors.ENDC}")
        print("=" * 60)
        print(f"1. {Colors.CYAN}Start new crawl{Colors.ENDC}")
        print(f"2. {Colors.CYAN}Search crawled content{Colors.ENDC}")
        print(f"3. {Colors.CYAN}Purge crawled data{Colors.ENDC}")
        print(f"4. {Colors.CYAN}View/modify configuration{Colors.ENDC}")
        print(f"a. {Colors.CYAN}Toggle auto-refresh (currently {'ON' if auto_refresh else 'OFF'}){Colors.ENDC}")
        print(f"m. {Colors.CYAN}Start master node{Colors.ENDC}")
        print(f"r. {Colors.CYAN}Manual refresh{Colors.ENDC}")
        print(f"q. {Colors.CYAN}Quit{Colors.ENDC}")

        # Restore cursor for first update
        sys.stdout.write(Cursor.RESTORE)

        # Save the lines where we need to update stats
        crawled_line = -11  # Lines from bottom
        crawl_queue_line = -10
        index_queue_line = -9

        import select  # For non-blocking input
        last_update = time.time()

        while True:
            # Check for user input (non-blocking)
            rlist, _, _ = select.select([sys.stdin], [], [], 0.1)

            current_time = time.time()
            if auto_refresh and current_time - last_update >= refresh_interval:
                last_update = current_time

                # Get updated stats
                stats = get_crawl_stats()

                # Only update if values changed
                if stats["crawled_pages"] != prev_stats["crawled_pages"]:
                    # Move cursor to crawled line and update
                    sys.stdout.write(f"\033[{crawled_line}F")
                    sys.stdout.write(f"Crawled Pages: {Colors.GREEN}{stats['crawled_pages']}{Colors.ENDC}{' ' * 20}")
                    sys.stdout.write(f"\033[{-crawled_line}E")
                    prev_stats["crawled_pages"] = stats["crawled_pages"]

                if stats["crawl_queue"] != prev_stats["crawl_queue"]:
                    # Move cursor to crawl queue line and update
                    sys.stdout.write(f"\033[{crawl_queue_line}F")
                    sys.stdout.write(f"Crawler Queue: {Colors.CYAN}{stats['crawl_queue']}{Colors.ENDC} tasks{' ' * 20}")
                    sys.stdout.write(f"\033[{-crawl_queue_line}E")
                    prev_stats["crawl_queue"] = stats["crawl_queue"]

                if stats["index_queue"] != prev_stats["index_queue"]:
                    # Move cursor to index queue line and update
                    sys.stdout.write(f"\033[{index_queue_line}F")
                    sys.stdout.write(f"Indexer Queue: {Colors.CYAN}{stats['index_queue']}{Colors.ENDC} tasks{' ' * 20}")
                    sys.stdout.write(f"\033[{-index_queue_line}E")
                    prev_stats["index_queue"] = stats["index_queue"]

                # Force flush
                sys.stdout.flush()

            if rlist:
                choice = sys.stdin.readline().strip()

                if choice == '1':
                    start_new_crawl()
                    # Refresh entire dashboard after returning
                    return show_dashboard()
                elif choice == '2':
                    search_interface()
                    # Refresh entire dashboard after returning
                    return show_dashboard()
                elif choice == '3':
                    purge_data()
                    # Refresh entire dashboard after returning
                    return show_dashboard()
                elif choice == '4':
                    modify_config()
                    # Refresh entire dashboard after returning
                    return show_dashboard()
                elif choice == 'a':
                    auto_refresh = not auto_refresh
                    # Update refresh status
                    refresh_text = f"a. {Colors.CYAN}Toggle auto-refresh (currently {'ON' if auto_refresh else 'OFF'}){Colors.ENDC}"
                    sys.stdout.write(f"\033[5F")  # Move up 5 lines
                    sys.stdout.write(refresh_text + " " * 20 + "\n")
                    sys.stdout.write(f"\033[4E")  # Move back down
                    sys.stdout.flush()
                elif choice == 'm':
                    # Start master node
                    print(f"\n{Colors.CYAN}Starting master node...{Colors.ENDC}")
                    if start_master_node():
                        print(f"{Colors.GREEN}Master node startup initiated.{Colors.ENDC}")
                        print(f"{Colors.CYAN}Waiting 5 seconds for node to initialize...{Colors.ENDC}")
                        time.sleep(5)  # Wait for startup
                    # Refresh entire dashboard
                    return show_dashboard()
                elif choice == 'r':
                    # Manual refresh - update all stats
                    stats = get_crawl_stats()

                    # Update all stats lines
                    sys.stdout.write(f"\033[{crawled_line}F")
                    sys.stdout.write(f"Crawled Pages: {Colors.GREEN}{stats['crawled_pages']}{Colors.ENDC}{' ' * 20}")
                    sys.stdout.write(f"\033[{-crawled_line}E")

                    sys.stdout.write(f"\033[{crawl_queue_line}F")
                    sys.stdout.write(f"Crawler Queue: {Colors.CYAN}{stats['crawl_queue']}{Colors.ENDC} tasks{' ' * 20}")
                    sys.stdout.write(f"\033[{-crawl_queue_line}E")

                    sys.stdout.write(f"\033[{index_queue_line}F")
                    sys.stdout.write(f"Indexer Queue: {Colors.CYAN}{stats['index_queue']}{Colors.ENDC} tasks{' ' * 20}")
                    sys.stdout.write(f"\033[{-index_queue_line}E")

                    sys.stdout.flush()

                    prev_stats = {
                        "crawled_pages": stats["crawled_pages"],
                        "crawl_queue": stats["crawl_queue"],
                        "index_queue": stats["index_queue"]
                    }
                elif choice == 'q':
                    break

    except KeyboardInterrupt:
        print(f"\n{Colors.CYAN}Exiting dashboard...{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.RED}Error in dashboard: {e}{Colors.ENDC}")
        input("Press Enter to continue...")

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