#!/usr/bin/env python3
"""
Simple UI for the distributed web crawler
This script provides a text-based interface for configuring and launching the crawler
"""
import os
import sys
import subprocess
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def validate_url(url):
    """Simple URL validation"""
    if not url.startswith(('http://', 'https://')):
        return False
    return True

def get_user_input():
    """Get configuration input from the user"""
    print("\n===== Distributed Web Crawler Configuration =====\n")

    # Get seed URLs
    print("Enter seed URLs (one per line, empty line to finish):")
    seed_urls = []
    while True:
        url = input("> ").strip()
        if not url:
            break

        if validate_url(url):
            seed_urls.append(url)
        else:
            print(f"Invalid URL: {url} - URLs must start with http:// or https://")

    if not seed_urls:
        seed_urls = ["https://example.com", "https://www.wikipedia.org", "https://www.github.com"]
        print(f"No valid URLs entered. Using defaults: {', '.join(seed_urls)}")

    # Get restricted URLs/domains
    print("\nEnter restricted URLs or domains to avoid (one per line, empty line to finish):")
    print("Examples: facebook.com, twitter.com, instagram.com")
    restricted_urls = []
    while True:
        url = input("> ").strip()
        if not url:
            break
        restricted_urls.append(url)

    # Get max depth
    max_depth = 3  # Default
    while True:
        try:
            depth_input = input("\nEnter maximum crawl depth [default=3]: ").strip()
            if not depth_input:
                break
            max_depth = int(depth_input)
            if max_depth < 1:
                print("Depth must be at least 1")
                continue
            break
        except ValueError:
            print("Please enter a valid number")

    # Get max URLs per domain
    max_urls_per_domain = 50  # Default
    while True:
        try:
            urls_input = input("\nEnter maximum URLs to crawl per domain [default=50]: ").strip()
            if not urls_input:
                break
            max_urls_per_domain = int(urls_input)
            if max_urls_per_domain < 1:
                print("Must crawl at least 1 URL per domain")
                continue
            break
        except ValueError:
            print("Please enter a valid number")

    # Get number of processes
    num_processes = 4  # Default (1 master + 2 crawlers + 1 indexer)
    while True:
        try:
            proc_input = input("\nEnter number of processes to use [default=4]: ").strip()
            if not proc_input:
                break
            num_processes = int(proc_input)
            if num_processes < 4:
                print("Need at least 4 processes (1 master + 2 crawlers + 1 indexer)")
                continue
            break
        except ValueError:
            print("Please enter a valid number")

    # Format the configuration
    config = {
        "seed_urls": seed_urls,
        "restricted_urls": restricted_urls,
        "max_depth": max_depth,
        "max_urls_per_domain": max_urls_per_domain,
        "num_processes": num_processes
    }

    # Review the configuration
    print("\n===== Crawler Configuration Summary =====")
    print(f"Seed URLs ({len(seed_urls)}):")
    for url in seed_urls:
        print(f"  - {url}")

    if restricted_urls:
        print(f"\nRestricted URLs/domains ({len(restricted_urls)}):")
        for url in restricted_urls:
            print(f"  - {url}")
    else:
        print("\nNo restricted URLs/domains")

    print(f"\nMaximum crawl depth: {max_depth}")
    print(f"Maximum URLs per domain: {max_urls_per_domain}")
    print(f"Number of processes: {num_processes}")

    return config

def launch_crawler(config):
    """Launch the distributed web crawler with the provided configuration"""
    try:
        # Save the configuration to a temporary file to avoid command line length limitations
        with open('crawler_config.json', 'w') as f:
            json.dump(config, f)

        # Build the MPI command
        command = [
            "mpiexec",
            "-n", str(config["num_processes"]),
            "python", os.path.join(os.path.dirname(__file__), "main.py"),
            "--config", "crawler_config.json"
        ]

        print("\nLaunching the distributed web crawler...")
        print(f"Command: {' '.join(command)}")

        # Execute the command
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # Stream the output
        print("\n===== Crawler Output =====")
        for line in process.stdout:
            print(line, end='')

        # Wait for the process to complete
        return_code = process.wait()

        if return_code == 0:
            print("\nCrawl completed successfully!")
        else:
            print(f"\nCrawl exited with error code: {return_code}")

        return return_code

    except Exception as e:
        logging.error(f"Error launching crawler: {e}")
        return 1

def main():
    """Main function to run the UI"""
    print("Welcome to the Distributed Web Crawler")

    while True:
        # Get configuration from user
        config = get_user_input()

        # Ask for confirmation
        confirm = input("\nStart crawling with this configuration? [Y/n]: ").strip().lower()
        if confirm in ['', 'y', 'yes']:
            # Create main.py if it doesn't exist
            main_path = os.path.join(os.path.dirname(__file__), "main.py")
            if not os.path.exists(main_path):
                create_main_script(main_path)

            # Launch the crawler
            result = launch_crawler(config)
            if result == 0:
                # Ask if the user wants to do another crawl
                again = input("\nDo you want to start another crawl? [y/N]: ").strip().lower()
                if again not in ['y', 'yes']:
                    break
            else:
                # If there was an error, ask if they want to try again
                again = input("\nThere was an error. Try again? [Y/n]: ").strip().lower()
                if again in ['n', 'no']:
                    break
        else:
            # Ask if they want to reconfigure
            reconfigure = input("Reconfigure the crawler? [Y/n]: ").strip().lower()
            if reconfigure in ['n', 'no']:
                break

    print("\nThank you for using the Distributed Web Crawler.")

def create_main_script(path):
    """Create the main.py script that combines all components"""
    with open(path, 'w') as f:
        f.write('''#!/usr/bin/env python3
"""
Main entry point for the distributed web crawler
This script coordinates master, crawler, and indexer nodes
"""
import os
import sys
import json
import argparse
from mpi4py import MPI
from master_node import master_process
from crawler_node import crawler_process
from indexer_node import indexer_process

def main():
    """Main entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Distributed Web Crawler")
    parser.add_argument("--config", help="Path to JSON configuration file")
    parser.add_argument("--seed-urls", nargs="+", help="Seed URLs to start crawling")
    parser.add_argument("--restricted-urls", nargs="+", help="Restricted URLs to avoid crawling")
    parser.add_argument("--max-depth", type=int, default=3, help="Maximum crawl depth")
    args = parser.parse_args()

    # Get the MPI rank
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # If config file is provided, load it
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config = json.load(f)

        # Override command line args with config file
        if not args.seed_urls and 'seed_urls' in config:
            args.seed_urls = config['seed_urls']
        if not args.restricted_urls and 'restricted_urls' in config:
            args.restricted_urls = config['restricted_urls']
        if args.max_depth == 3 and 'max_depth' in config:  # Only override if default
            args.max_depth = config['max_depth']

    # Start the appropriate process based on rank
    if rank == 0:
        # Master node
        master_process()
    elif rank <= 2:
        # Crawler nodes
        crawler_process()
    else:
        # Indexer node
        indexer_process()

if __name__ == "__main__":
    main()
''')

if __name__ == "__main__":
    main()