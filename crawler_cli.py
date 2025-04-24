#!/usr/bin/env python3
# filepath: crawler_cli.py

import argparse
import json
import os
import sys
import time
import threading
from crawler_config import CrawlerConfig
from coordinator import start_crawl
from celery.result import AsyncResult
from celery_app import app as celery_app
from search import search_content

def load_config():
    """Load configuration from config file and S3"""
    config_manager = CrawlerConfig()
    return config_manager.get_config()

def save_config(config):
    """Save configuration to config file and S3"""
    config_manager = CrawlerConfig()
    config_manager.config = config
    config_manager.save_config()
    print("Configuration saved successfully")

def print_config(config):
    """Print current configuration"""
    print("\nCurrent Configuration:")
    print("-" * 50)
    print(f"Seed URLs: {', '.join(config['seed_urls'])}")
    print(f"Restricted Domains: {', '.join(config['restricted_domains'])}")
    print(f"Max Depth: {config['max_depth']}")
    print(f"Number of Crawlers: {config['num_crawlers']}")
    print(f"Number of Indexers: {config['num_indexers']}")
    print(f"Request Delay: {config['request_delay']} seconds")
    print(f"Timeout: {config['timeout']} seconds")
    print(f"Output Directory: {config['output_dir']}")
    print(f"Elasticsearch URL: {config.get('elasticsearch_url', 'http://localhost:9200')}")
    print(f"Elasticsearch Index: {config.get('elasticsearch_index', 'webcrawler')}")
    print("-" * 50)

def monitor_tasks(task_ids, max_runtime=300, status_interval=5):
    """Monitor task progress by watching S3 (SQS compatible)"""
    print(f"\nMonitoring {len(task_ids)} crawler tasks...")

    try:
        # Use S3 monitoring instead of AsyncResult/Inspector
        from aws_config import S3_BUCKET_NAME, S3_OUTPUT_PREFIX, ensure_aws_clients
        ensure_aws_clients()
        from aws_config import s3_client

        # Get initial count of objects in S3
        initial_count = count_s3_objects(S3_OUTPUT_PREFIX)
        print(f"Initial content count in S3: {initial_count}")

        # Monitor progress with overall timeout
        start_time = time.time()

        while True:
            # Check timeouts
            elapsed_time = time.time() - start_time
            if elapsed_time > max_runtime:
                print("\nMaximum runtime exceeded. Stopping monitoring.")
                break

            # Check S3 for new content
            current_count = count_s3_objects(S3_OUTPUT_PREFIX)
            new_items = current_count - initial_count

            print(f"\r[{time.strftime('%H:%M:%S')}] Processing... S3 objects: {current_count} " +
                  f"(+{new_items} since start)", end="", flush=True)

            # If no new items for a while and some time has passed, assume completion
            if elapsed_time > 60 and new_items == 0:
                print("\nNo new content for a minute. Assuming completion.")
                break

            time.sleep(status_interval)
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nError in monitoring: {e}")

# Add the S3 counting function as well
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

def start_crawler(args):
    """Start the crawler with the given configuration"""
    config = load_config()

    # Update config from command line arguments
    if args.seed_urls:
        config['seed_urls'] = args.seed_urls

    if args.max_depth is not None:
        config['max_depth'] = args.max_depth

    if args.num_crawlers is not None:
        config['num_crawlers'] = args.num_crawlers

    if args.num_indexers is not None:
        config['num_indexers'] = args.num_indexers

    if args.request_delay is not None:
        config['request_delay'] = args.request_delay

    if args.timeout is not None:
        config['timeout'] = args.timeout

    if args.output_dir:
        config['output_dir'] = args.output_dir

    # Save updated config
    save_config(config)
    print_config(config)

    # Start the crawler
    print(f"\nStarting distributed web crawler with Celery...")
    print(f"Using {config['num_crawlers']} crawler workers and {config['num_indexers']} indexer workers")

    try:
        # Check node health
        check_node_health()

        # Submit initial tasks
        task_ids = start_crawl()
        print(f"Submitted {len(task_ids)} initial crawling tasks")

        # Monitor tasks if requested
        if not args.no_monitor:
            monitor_tasks(task_ids)

    except Exception as e:
        print(f"Error starting crawler: {e}")

def check_node_health():
    """Check if crawler and indexer nodes are healthy"""
    try:
        from distributed_config import CRAWLER_IP, INDEXER_IP
        import requests

        # Check crawler node
        try:
            crawler_resp = requests.get(f"http://{CRAWLER_IP}:8080/health", timeout=5)
            crawler_status = "OK" if crawler_resp.status_code == 200 else "ERROR"
        except Exception:
            crawler_status = "DOWN"

        # Check indexer node
        try:
            indexer_resp = requests.get(f"http://{INDEXER_IP}:8080/health", timeout=5)
            indexer_status = "OK" if indexer_resp.status_code == 200 else "ERROR"
        except Exception:
            indexer_status = "DOWN"

        print(f"Node Health Check - Crawler: {crawler_status}, Indexer: {indexer_status}")

        if crawler_status != "OK":
            print("Warning: Crawler node appears to be down or unhealthy.")
        if indexer_status != "OK":
            print("Warning: Indexer node appears to be down or unhealthy.")
    except Exception as e:
        print(f"Error checking node health: {e}")

def show_status(args):
    """Show the status of current crawler tasks using AWS-friendly approaches"""
    try:
        print("\nCrawler Status")
        print("-" * 50)
        print("Note: Detailed task inspection is not available with AWS SQS.")

        # Show S3 storage status
        from aws_config import S3_BUCKET_NAME, S3_OUTPUT_PREFIX, ensure_aws_clients
        ensure_aws_clients()

        from aws_config import s3_client

        try:
            # Count processed URLs in S3
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=S3_OUTPUT_PREFIX
            )

            processed_count = 0
            if 'Contents' in response:
                processed_count = sum(1 for item in response['Contents'] if item['Key'].endswith('.json'))

            print(f"\nProcessed URLs (in S3): {processed_count}")
            print(f"S3 Bucket: {S3_BUCKET_NAME}")

            # Check crawler and indexer node health through API endpoints
            from distributed_config import CRAWLER_IP, INDEXER_IP
            import requests

            try:
                crawler_status = "UNKNOWN"
                try:
                    r = requests.get(f"http://{CRAWLER_IP}:8080/health", timeout=2)
                    crawler_status = "RUNNING" if r.status_code == 200 else "ERROR"
                except:
                    crawler_status = "DOWN"

                indexer_status = "UNKNOWN"
                try:
                    r = requests.get(f"http://{INDEXER_IP}:8080/health", timeout=2)
                    indexer_status = "RUNNING" if r.status_code == 200 else "ERROR"
                except:
                    indexer_status = "DOWN"

                print(f"\nCrawler node: {crawler_status}")
                print(f"Indexer node: {indexer_status}")
            except:
                print("Could not check node health")

            # Try to check OpenSearch status
            try:
                from distributed_config import OPENSEARCH_ENDPOINT
                from requests_aws4auth import AWS4Auth
                from distributed_config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, OPENSEARCH_USER, OPENSEARCH_PASS
                import requests

                opensearch_status = "DOWN"

                # Determine auth method
                auth_method = "aws4auth"  # Default
                try:
                    with open("opensearch_auth_method.txt", "r") as f:
                        auth_method = f.read().strip()
                except:
                    pass

                if auth_method == "aws4auth":
                    auth = AWS4Auth(
                        AWS_ACCESS_KEY_ID,
                        AWS_SECRET_ACCESS_KEY,
                        AWS_REGION,
                        'es'
                    )

                    try:
                        response = requests.get(
                            f"{OPENSEARCH_ENDPOINT}/_cluster/health",
                            auth=auth,
                            timeout=3
                        )
                        opensearch_status = "OK" if response.status_code == 200 else "ERROR"
                    except:
                        opensearch_status = "DOWN"
                else:
                    # Basic auth
                    try:
                        response = requests.get(
                            f"{OPENSEARCH_ENDPOINT}/_cluster/health",
                            auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
                            timeout=3
                        )
                        opensearch_status = "OK" if response.status_code == 200 else "ERROR"
                    except:
                        opensearch_status = "DOWN"

                print(f"OpenSearch status: {opensearch_status}")

            except Exception as es_err:
                print(f"Could not check OpenSearch status: {es_err}")

        except Exception as e:
            print(f"Error getting S3 details: {e}")

        print("-" * 50)

    except Exception as e:
        print(f"Error getting status: {e}")

def search_crawler(args):
    """Search indexed content"""
    if not args.query:
        print("Error: Query is required for search")
        return

    print(f"Searching for: {args.query}")

    try:
        results = search_content(args.query)

        if not results:
            print("No results found.")
            return

        print(f"\nFound {len(results)} results:\n")

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']} (Score: {result['score']:.2f})")
            print(f"   URL: {result['url']}")
            if 's3_key' in result and result['s3_key']:
                print(f"   S3: {result['s3_key']}")
            print(f"   Description: {result['description'][:100]}..." if len(result['description']) > 100 else result['description'])

            if "highlights" in result and "text_content" in result["highlights"]:
                print("   Highlights:")
                for highlight in result["highlights"]["text_content"]:
                    print(f"   - ...{highlight}...")

            print()

    except Exception as e:
        print(f"Error searching: {e}")

def configure(args):
    """Configure crawler settings"""
    config = load_config()
    print_config(config)

    if args.interactive:
        # Interactive configuration
        print("\nEnter new configuration values (press Enter to keep current value):")

        # Seed URLs
        seed_input = input(f"Seed URLs (comma-separated, current: {', '.join(config['seed_urls'])}): ")
        if seed_input.strip():
            config['seed_urls'] = [url.strip() for url in seed_input.split(',')]

        # Max depth
        depth_input = input(f"Max depth (current: {config['max_depth']}): ")
        if depth_input.strip():
            config['max_depth'] = int(depth_input)

        # Number of crawlers
        crawlers_input = input(f"Number of crawlers (current: {config['num_crawlers']}): ")
        if crawlers_input.strip():
            config['num_crawlers'] = int(crawlers_input)

        # Number of indexers
        indexers_input = input(f"Number of indexers (current: {config['num_indexers']}): ")
        if indexers_input.strip():
            config['num_indexers'] = int(indexers_input)

        # Request delay
        delay_input = input(f"Request delay in seconds (current: {config['request_delay']}): ")
        if delay_input.strip():
            config['request_delay'] = float(delay_input)

        # Timeout
        timeout_input = input(f"Request timeout in seconds (current: {config['timeout']}): ")
        if timeout_input.strip():
            config['timeout'] = int(timeout_input)

        # Output directory
        output_input = input(f"Output directory (current: {config['output_dir']}): ")
        if output_input.strip():
            config['output_dir'] = output_input

        # Save the updated configuration
        save_config(config)
        print_config(config)
    else:
        # Display current config only
        pass

def list_s3_content(args):
    """List content in S3 bucket"""
    from aws_config import S3_BUCKET_NAME, S3_OUTPUT_PREFIX, ensure_aws_clients

    ensure_aws_clients()
    from aws_config import s3_client

    print(f"\nContent in S3 bucket: {S3_BUCKET_NAME}")
    print("-" * 50)

    try:
        # List objects in the bucket with the given prefix
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_OUTPUT_PREFIX
        )

        if 'Contents' not in response or not response['Contents']:
            print("No content found in S3 bucket.")
            return

        # Count and display statistics
        json_files = [item for item in response['Contents'] if item['Key'].endswith('.json')]
        txt_files = [item for item in response['Contents'] if item['Key'].endswith('.txt')]

        print(f"Found {len(json_files)} JSON files and {len(txt_files)} text files")
        print(f"Total storage used: {sum(item['Size'] for item in response['Contents']) / (1024*1024):.2f} MB")

        # Display the most recent files
        if json_files:
            sorted_files = sorted(json_files, key=lambda x: x['LastModified'], reverse=True)
            print("\nMost recent crawled pages:")
            for i, item in enumerate(sorted_files[:10], 1):
                key = item['Key']
                size_kb = item['Size'] / 1024
                last_modified = item['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"{i}. {key} ({size_kb:.1f} KB, {last_modified})")

            if len(sorted_files) > 10:
                print(f"... and {len(sorted_files) - 10} more files")
    except Exception as e:
        print(f"Error listing S3 content: {e}")

def fix_resources(args):
    """Fix or initialize AWS resources"""
    print("Fixing AWS resources...")

    try:
        # Initialize AWS resources
        from aws_config import setup_aws_resources, fix_dynamodb_table

        # Set up resources
        if setup_aws_resources():
            print("AWS resources set up successfully.")
        else:
            print("Warning: Some AWS resources could not be set up.")

        # Fix DynamoDB table
        if fix_dynamodb_table():
            print("DynamoDB table fixed successfully.")
        else:
            print("Error: Could not fix DynamoDB table.")

        # Test OpenSearch connection
        from aws_config import test_opensearch_connection
        success, auth_method = test_opensearch_connection()
        if success:
            print(f"OpenSearch connection tested successfully using {auth_method} authentication.")
        else:
            print("Warning: Could not connect to OpenSearch.")

    except Exception as e:
        print(f"Error fixing AWS resources: {e}")

def main():
    """Main entry point for the CLI"""
    parser = argparse.ArgumentParser(description="Distributed Web Crawler CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start the crawler")
    start_parser.add_argument("--seed-urls", nargs="+", help="Seed URLs to start crawling from")
    start_parser.add_argument("--max-depth", type=int, help="Maximum crawl depth")
    start_parser.add_argument("--num-crawlers", type=int, help="Number of crawler workers")
    start_parser.add_argument("--num-indexers", type=int, help="Number of indexer workers")
    start_parser.add_argument("--request-delay", type=float, help="Delay between requests in seconds")
    start_parser.add_argument("--timeout", type=int, help="Request timeout in seconds")
    start_parser.add_argument("--output-dir", help="Output directory for crawled content")
    start_parser.add_argument("--no-monitor", action="store_true", help="Don't monitor tasks after starting")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show crawler status")

    # Search command
    search_parser = subparsers.add_parser("search", help="Search indexed content")
    search_parser.add_argument("query", help="Search query")

    # List command for S3
    list_parser = subparsers.add_parser("list", help="List crawled content in S3")

    # Configure command
    config_parser = subparsers.add_parser("config", help="Configure crawler settings")
    config_parser.add_argument("--interactive", "-i", action="store_true", help="Interactive configuration")

    # Fix command for AWS resources
    fix_parser = subparsers.add_parser("fix", help="Fix/initialize AWS resources")

    args = parser.parse_args()

    if args.command == "start":
        start_crawler(args)
    elif args.command == "status":
        show_status(args)
    elif args.command == "search":
        search_crawler(args)
    elif args.command == "config":
        configure(args)
    elif args.command == "list":
        list_s3_content(args)
    elif args.command == "fix":
        fix_resources(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()