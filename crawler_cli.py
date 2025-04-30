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
    """Monitor task progress and shutdown nodes when crawling is complete"""
    print(f"\nMonitoring {len(task_ids)} crawler tasks...")

    try:
        from aws_config import S3_BUCKET_NAME, S3_OUTPUT_PREFIX, ensure_aws_clients
        ensure_aws_clients()
        from aws_config import s3_client

        # Get initial count
        initial_count = count_s3_objects(S3_OUTPUT_PREFIX)
        print(f"Initial content count in S3: {initial_count}")

        # Track when count stabilizes
        stable_count = initial_count
        stable_since = time.time()
        no_change_duration = 0

        # Monitor progress with overall timeout
        start_time = time.time()

        while True:
            # Check timeouts
            elapsed_time = time.time() - start_time
            if elapsed_time > max_runtime:
                print("\nMaximum runtime exceeded. Initiating shutdown sequence.")
                trigger_shutdown()
                break

            # Sleep before checking again
            time.sleep(status_interval)

            # Check S3 for new content
            current_count = count_s3_objects(S3_OUTPUT_PREFIX)
            new_items = current_count - initial_count

            # If count changed, reset the stability timer
            if current_count != stable_count:
                stable_count = current_count
                stable_since = time.time()
                no_change_duration = 0
            else:
                no_change_duration = time.time() - stable_since

            print(f"\r[{time.strftime('%H:%M:%S')}] Processing... S3 objects: {current_count} " +
                  f"(+{new_items} since start, stable for {int(no_change_duration)}s)", end="", flush=True)

            # If no new items for 2 minutes and some processing has occurred, assume completion
            if no_change_duration > 120 and current_count > initial_count:
                print("\nNo new content for 2 minutes. Crawling complete. Initiating shutdown sequence.")
                trigger_shutdown()
                break

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nError in monitoring: {e}")

def trigger_shutdown():
    """Send shutdown signals to crawler and indexer nodes"""
    from distributed_config import CRAWLER_IP, INDEXER_IP
    import requests

    print("\n==== Initiating Graceful Shutdown Sequence ====")

    # Step 1: Shutdown crawler nodes
    try:
        print("Sending shutdown signal to crawler node...")
        resp = requests.post(f"http://{CRAWLER_IP}:8080/shutdown", timeout=3)
        print(f"Crawler node shutdown response: {resp.status_code}")
    except Exception as e:
        print(f"Error shutting down crawler node: {e}")

    # Wait a few seconds for crawler to finish
    print("Waiting for crawler node to shutdown...")
    time.sleep(5)

    # Step 2: Shutdown indexer nodes
    try:
        print("Sending shutdown signal to indexer node...")
        resp = requests.post(f"http://{INDEXER_IP}:8080/shutdown", timeout=3)
        print(f"Indexer node shutdown response: {resp.status_code}")
    except Exception as e:
        print(f"Error shutting down indexer node: {e}")

    # Step 3: Shutdown master
    print("Shutting down master node...")
    time.sleep(3)  # Give time to print messages
    os._exit(0)  # Exit without waiting for threads


def check_queue_status():
    """Check if there are any pending tasks in the queues"""
    try:
        from aws_config import sqs_client, SQS_CRAWLER_QUEUE_NAME, SQS_INDEXER_QUEUE_NAME

        # Get queue URLs
        crawler_queue_url = sqs_client.get_queue_url(QueueName=SQS_CRAWLER_QUEUE_NAME)['QueueUrl']
        indexer_queue_url = sqs_client.get_queue_url(QueueName=SQS_INDEXER_QUEUE_NAME)['QueueUrl']

        # Get approximate number of messages
        crawler_attrs = sqs_client.get_queue_attributes(
            QueueUrl=crawler_queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        indexer_attrs = sqs_client.get_queue_attributes(
            QueueUrl=indexer_queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )

        # Count total messages (both visible and in flight)
        crawler_messages = (
            int(crawler_attrs['Attributes']['ApproximateNumberOfMessages']) +
            int(crawler_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        )
        indexer_messages = (
            int(indexer_attrs['Attributes']['ApproximateNumberOfMessages']) +
            int(indexer_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        )

        return {
            "crawler_queue": crawler_messages,
            "indexer_queue": indexer_messages,
            "total_pending": crawler_messages + indexer_messages
        }
    except Exception as e:
        print(f"Error checking queue status: {e}")
        return {"error": str(e)}


def mark_crawl_as_complete(task_ids):
    """Mark that a crawl session is complete in S3"""
    from aws_config import S3_BUCKET_NAME, ensure_aws_clients
    ensure_aws_clients()
    from aws_config import s3_client

    try:
        completion_data = {
            "completed_at": time.time(),
            "task_ids": task_ids,
            "status": "completed"
        }

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key="status/crawl_completed.json",
            Body=json.dumps(completion_data),
            ContentType='application/json'
        )
        print("Crawl session marked as complete in S3")
    except Exception as e:
        print(f"Error marking crawl as complete: {e}")

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



def purge_data(args):
    """Purge all crawled data from S3, DynamoDB and OpenSearch"""
    if not args.force:
        print("\n⚠️ WARNING: This will permanently delete all crawled data! ⚠️")
        print("This includes:")
        print("  - All content stored in S3 bucket")
        print("  - All task data in DynamoDB table")
        print("  - All indexed content in OpenSearch")

        confirm = input("\nType 'yes' to confirm purge: ").strip().lower()
        if confirm != 'yes':
            print("Purge operation cancelled.")
            return

    print("Starting data purge operation...")
    success = True

    # 1. Purge S3 content
    try:
        print("Purging content from S3...")
        from aws_config import S3_BUCKET_NAME, S3_OUTPUT_PREFIX, ensure_aws_clients
        ensure_aws_clients()
        from aws_config import s3_client

        # List objects in output directory
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_OUTPUT_PREFIX
        )

        # Count total objects
        total_objects = 0
        for page in page_iterator:
            if 'Contents' in page:
                total_objects += len(page['Contents'])

        if total_objects == 0:
            print("No content found in S3 output directory.")
        else:
            print(f"Found {total_objects} objects to delete")

            # Delete objects in batches
            deleted_count = 0
            for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=S3_OUTPUT_PREFIX):
                if 'Contents' in page:
                    objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                    s3_client.delete_objects(
                        Bucket=S3_BUCKET_NAME,
                        Delete={'Objects': objects_to_delete}
                    )
                    deleted_count += len(objects_to_delete)
                    print(f"Deleted {deleted_count}/{total_objects} objects...", end="\r")
            print(f"\nSuccessfully deleted {deleted_count} objects from S3.")
    except Exception as e:
        print(f"Error purging S3 data: {e}")
        success = False

    # 2. Clear DynamoDB table
    try:
        print("\nClearing DynamoDB table...")
        from aws_config import DYNAMODB_TABLE_NAME, ensure_aws_clients
        ensure_aws_clients()
        from aws_config import dynamodb_client

        # Instead of deleting individual items, recreate the table
        # This is much faster for complete purges
        from aws_config import fix_dynamodb_table
        if fix_dynamodb_table(force_recreate=True):
            print("Successfully recreated DynamoDB table.")
        else:
            print("Failed to recreate DynamoDB table.")
            success = False
    except Exception as e:
        print(f"Error clearing DynamoDB table: {e}")
        success = False

    # 3. Clear OpenSearch index
    try:
        print("\nClearing OpenSearch index...")
        from distributed_config import OPENSEARCH_ENDPOINT

        if OPENSEARCH_ENDPOINT:
            # Determine which authentication method to use
            auth_method = "aws4auth"  # Default
            try:
                with open("opensearch_auth_method.txt", "r") as f:
                    auth_method = f.read().strip()
            except (FileNotFoundError, IOError):
                pass

            # Load config to get index name
            from crawler_config import CrawlerConfig
            config = CrawlerConfig().get_config()
            index_name = config.get('elasticsearch_index', 'webcrawler')

            # Get OpenSearch connection
            from distributed_config import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
            from distributed_config import OPENSEARCH_USER, OPENSEARCH_PASS
            from requests_aws4auth import AWS4Auth
            import requests

            if auth_method == "aws4auth":
                auth = AWS4Auth(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, 'es')
                delete_response = requests.delete(
                    f"{OPENSEARCH_ENDPOINT}/{index_name}",
                    auth=auth
                )
            else:
                delete_response = requests.delete(
                    f"{OPENSEARCH_ENDPOINT}/{index_name}",
                    auth=(OPENSEARCH_USER, OPENSEARCH_PASS)
                )

            if delete_response.status_code in [200, 404]:
                print(f"Successfully deleted OpenSearch index '{index_name}'.")
            else:
                print(f"Failed to delete OpenSearch index: {delete_response.text}")
                success = False
        else:
            print("OpenSearch endpoint not configured, skipping index deletion.")
    except Exception as e:
        print(f"Error clearing OpenSearch index: {e}")
        success = False

    if success:
        print("\n✅ Data purge completed successfully! Ready for a fresh crawl.")
    else:
        print("\n⚠️ Data purge completed with some errors. Check the logs for details.")


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

        # Purge command
    purge_parser = subparsers.add_parser("purge", help="Purge all crawled data from S3, DynamoDB, and OpenSearch")
    purge_parser.add_argument("--force", "-f", action="store_true", help="Skip confirmation prompt")

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
    elif args.command == "purge":
        purge_data(args)
    elif args.command == "fix":
        fix_resources(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()