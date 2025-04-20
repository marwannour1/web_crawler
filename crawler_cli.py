#!/usr/bin/env python3
# filepath: g:\ain_shams\courses\Distributed Computing CSE354\projects\web_crawler\crawler_cli.py

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
    """Load configuration from config file"""
    config_manager = CrawlerConfig()
    return config_manager.get_config()

def save_config(config):
    """Save configuration to config file"""
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
    """Monitor task progress with timeout"""
    print(f"\nMonitoring {len(task_ids)} crawler tasks...")

    try:
        # Get initial tasks
        initial_tasks = [AsyncResult(task_id, app=celery_app) for task_id in task_ids]

        # Monitor task progress with overall timeout
        start_time = time.time()

        while True:
            # Get active task count from Celery inspector
            inspector = celery_app.control.inspect()
            active_tasks = inspector.active()
            reserved_tasks = inspector.reserved()

            # Count active tasks
            active_count = 0

            if active_tasks:
                active_count += sum(len(tasks) for tasks in active_tasks.values())

            if reserved_tasks:
                active_count += sum(len(tasks) for tasks in reserved_tasks.values())

            print(f"\r[{time.strftime('%H:%M:%S')}] Active tasks: {active_count}   ", end="", flush=True)

            # Check if all initial tasks are complete
            all_initial_done = all(task.ready() for task in initial_tasks)

            # Check timeouts
            elapsed_time = time.time() - start_time
            if elapsed_time > max_runtime:
                print("\nMaximum runtime exceeded. Stopping monitoring.")
                break

            if active_count == 0 and all_initial_done:
                print("\nAll tasks completed. Crawler process finished!")
                break

            time.sleep(status_interval)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nError in monitoring: {e}")

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
        # Submit initial tasks
        task_ids = start_crawl()
        print(f"Submitted {len(task_ids)} initial crawling tasks")

        # Monitor tasks if requested
        if not args.no_monitor:
            monitor_tasks(task_ids)

    except Exception as e:
        print(f"Error starting crawler: {e}")

def show_status(args):
    """Show the status of current crawler tasks"""
    try:
        # Get inspector
        inspector = celery_app.control.inspect()

        # Get various task statuses
        active = inspector.active()
        scheduled = inspector.scheduled()
        reserved = inspector.reserved()

        print("\nCrawler Status")
        print("-" * 50)

        # Active tasks
        active_count = 0
        if active:
            for worker, tasks in active.items():
                active_count += len(tasks)
                print(f"\nWorker {worker}: {len(tasks)} active tasks")
                for i, task in enumerate(tasks[:5], 1):  # Show first 5 tasks
                    print(f"  {i}. {task['name']} - {task['id'][:8]}...")
                if len(tasks) > 5:
                    print(f"  ... and {len(tasks) - 5} more")

        # Reserved tasks
        reserved_count = 0
        if reserved:
            reserved_count = sum(len(tasks) for tasks in reserved.values())

        # Scheduled tasks
        scheduled_count = 0
        if scheduled:
            scheduled_count = sum(len(tasks) for tasks in scheduled.values())

        print(f"\nSummary: {active_count} active, {reserved_count} reserved, {scheduled_count} scheduled tasks")
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

    # Configure command
    config_parser = subparsers.add_parser("config", help="Configure crawler settings")
    config_parser.add_argument("--interactive", "-i", action="store_true", help="Interactive configuration")

    args = parser.parse_args()

    if args.command == "start":
        start_crawler(args)
    elif args.command == "status":
        show_status(args)
    elif args.command == "search":
        search_crawler(args)
    elif args.command == "config":
        configure(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()