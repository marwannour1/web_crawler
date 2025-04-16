#!/usr/bin/env python3
from mpi4py import MPI
import logging
import time
import sys
from collections import deque
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def master_process(config=None):
    """Master node that coordinates crawlers and indexers"""
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank != 0:
        return  # Only rank 0 should execute this function

    logging.info(f"Master node starting. Total processes: {size}")

    # Use configuration if provided, otherwise use defaults
    if config is None:
        config = {
            'seed_urls': [
                "https://example.com",
                "https://www.wikipedia.org",
                "https://www.github.com"
            ],
            'num_crawlers': 2,
            'num_indexers': 1,
            'max_depth': 3,
            'restricted_domains': [],
            'output_dir': 'output'
        }

    # Create output directory if it doesn't exist
    os.makedirs(config['output_dir'], exist_ok=True)

    # Determine number of crawlers and indexers
    num_crawlers = config['num_crawlers']
    num_indexers = config['num_indexers']

    if size < 1 + num_crawlers + num_indexers:
        logging.error(f"Not enough processes. Need at least {1 + num_crawlers + num_indexers}")
        comm.Abort(1)

    # URL queue with seed URLs from config
    urls_to_crawl = deque([(url, 0) for url in config['seed_urls']])  # (url, depth)

    # Track crawler status (0=idle, 1=busy)
    crawler_status = {i: 0 for i in range(1, num_crawlers + 1)}

    # Track indexer status (0=idle, 1=busy)
    indexer_status = {i: 0 for i in range(1 + num_crawlers, 1 + num_crawlers + num_indexers)}

    # Main coordination loop
    while urls_to_crawl or any(crawler_status.values()) or any(indexer_status.values()):
        # Check for messages from crawlers and indexers
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
            status = MPI.Status()
            message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()

            logging.info(f"Master received message from rank {source}, tag {tag}")

            # Handle messages based on tags
            if tag == 1:  # Crawler finished and sending new URLs
                crawler_status[source] = 0  # Mark crawler as idle

                # Add new URLs to the queue if within depth limit
                current_depth = message.get('depth', 0)
                if current_depth < config['max_depth']:
                    new_urls = message.get('new_urls', [])
                    for url in new_urls:
                        # Check domain restrictions if any
                        allowed = True
                        if config['restricted_domains']:
                            allowed = any(domain in url for domain in config['restricted_domains'])

                        if allowed:
                            urls_to_crawl.append((url, current_depth + 1))
                            logging.info(f"Added new URL to queue: {url} (depth {current_depth + 1})")

                # Send content to an available indexer
                for indexer_rank, status in indexer_status.items():
                    if status == 0:  # If indexer is idle
                        indexer_status[indexer_rank] = 1  # Mark as busy
                        comm.send(message.get('content', ''), dest=indexer_rank, tag=2)
                        logging.info(f"Sent content to indexer {indexer_rank}")
                        break

            elif tag == 99:  # Indexer finished
                indexer_status[source] = 0  # Mark indexer as idle

            elif tag == 999:  # Error reported
                logging.error(f"Error reported by rank {source}: {message}")

                # Basic error handling - if crawler fails, mark as idle
                if source in crawler_status:
                    crawler_status[source] = 0

                # If indexer fails, mark as idle
                if source in indexer_status:
                    indexer_status[source] = 0

        # Assign URLs to idle crawlers
        for crawler_rank, status in crawler_status.items():
            if status == 0 and urls_to_crawl:  # If crawler is idle and we have URLs
                url, depth = urls_to_crawl.popleft()
                crawler_status[crawler_rank] = 1  # Mark as busy
                # Send URL and its current depth
                comm.send({'url': url, 'depth': depth}, dest=crawler_rank, tag=0)
                logging.info(f"Assigned URL {url} (depth {depth}) to crawler {crawler_rank}")

        time.sleep(0.1)  # Prevent busy waiting

    # Terminate all crawlers and indexers
    for i in range(1, size):
        comm.send("TERMINATE", dest=i, tag=10)

    logging.info("Master process complete. All URLs processed.")

if __name__ == '__main__':
    master_process()