#!/usr/bin/env python3
"""
Master node for the distributed web crawler
This node manages the URL queue and coordinates crawler and indexer nodes
"""
import time
import sys
import logging
import urllib.parse
from collections import defaultdict, deque
from mpi4py import MPI
import redis
from monitor import send_status_update

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def extract_domain(url):
    """Extract the domain from a URL"""
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc

def should_crawl_url(url, restricted_domains, crawled_urls, domain_counts, max_per_domain):
    """Determine if a URL should be crawled based on various criteria"""
    # Skip if already crawled
    if url in crawled_urls:
        return False

    # Check against restricted domains
    domain = extract_domain(url)
    for restricted in restricted_domains:
        if restricted in domain:
            return False

    # Check domain limit
    if domain_counts[domain] >= max_per_domain:
        return False

    return True

def master_process(config=None):
    """Master node process implementation"""
    if not config:
        config = {}

    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # This process should only run on the master node (rank 0)
    if rank != 0:
        return

    logging.info(f"Master node starting. Total processes: {size}")

    # Initialize Redis for monitoring
    try:
        redis_client = redis.Redis(host='172.28.79.201', port=6379, db=0)
        send_status_update(redis_client, 'master', 0, 'Starting', queue_size=0)
    except Exception as e:
        logging.error(f"Error connecting to Redis: {e}")
        redis_client = None

    # Extract configuration
    seed_urls = config.get('seed_urls', [])
    restricted_urls = config.get('restricted_urls', [])
    max_depth = config.get('max_depth', 3)
    max_urls_per_domain = config.get('max_urls_per_domain', 50)

    # Initialize tracking structures
    url_queue = deque()  # URLs to crawl
    crawled_urls = set()  # URLs already crawled
    url_depths = {}  # Track depth of each URL
    domain_counts = defaultdict(int)  # Count URLs per domain
    crawler_status = {i: 'idle' for i in range(1, size-1)}  # Track crawler status

    # Add seed URLs to the queue
    for url in seed_urls:
        url_queue.append(url)
        url_depths[url] = 0  # Seed URLs are at depth 0

    # Report initial queue size
    if redis_client:
        send_status_update(redis_client, 'master', 0, 'Queue initialized',
                          queue_size=len(url_queue))

    # Track statistics
    start_time = time.time()
    urls_queued = len(seed_urls)
    urls_crawled = 0
    urls_errored = 0

    # Variables to track node statuses
    active_crawlers = 0
    idle_crawlers = size - 2  # All crawlers start idle

    # Main processing loop
    while True:
        # Check if we should terminate (queue empty and all crawlers idle)
        if not url_queue and active_crawlers == 0:
            # All work is done, send termination signal to crawlers
            for i in range(1, size-1):
                comm.send(None, dest=i, tag=10)  # Tag 10 for termination
                logging.info(f"Sent termination signal to crawler {i}")

            # Send termination to indexer
            comm.send(None, dest=size-1, tag=10)
            logging.info("Sent termination signal to indexer")

            # Report completion
            if redis_client:
                elapsed_time = time.time() - start_time
                send_status_update(redis_client, 'master', 0, 'Completed',
                                  queue_size=0,
                                  urls_crawled=urls_crawled,
                                  urls_errored=urls_errored,
                                  elapsed_time=elapsed_time)

            logging.info(f"Crawl completed. Processed {urls_crawled} URLs in {elapsed_time:.2f} seconds")
            break

        # Check for messages from crawler nodes
        status = MPI.Status()
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()

            if tag == 1:  # Crawler results
                result = comm.recv(source=source, tag=tag)
                url = result['url']
                new_urls = result['new_urls']
                content = result['content']
                status = result['status']

                # Mark this URL as crawled
                crawled_urls.add(url)
                urls_crawled += 1

                if status == 'success':
                    # Update crawler status
                    crawler_status[source] = 'idle'
                    active_crawlers -= 1
                    idle_crawlers += 1

                    # Current depth of this URL
                    current_depth = url_depths[url]

                    # Only process new URLs if we haven't reached max depth
                    if current_depth < max_depth:
                        next_depth = current_depth + 1
                        for new_url in new_urls:
                            domain = extract_domain(new_url)

                            # Check if we should crawl this URL
                            if new_url not in crawled_urls and new_url not in url_queue and domain_counts[domain] < max_urls_per_domain:
                                url_queue.append(new_url)
                                url_depths[new_url] = next_depth
                                domain_counts[domain] += 1
                                urls_queued += 1

                    # Send content to indexer
                    if content:
                        indexer_data = {
                            'url': url,
                            'content': content
                        }
                        comm.send(indexer_data, dest=size-1, tag=2)  # Tag 2 for indexer data

                elif status == 'error' or status.startswith('http_error'):
                    # Update crawler status
                    crawler_status[source] = 'idle'
                    active_crawlers -= 1
                    idle_crawlers += 1
                    urls_errored += 1

                # Report status update
                if redis_client:
                    send_status_update(redis_client, 'master', 0, 'Processing queue',
                                      queue_size=len(url_queue),
                                      urls_crawled=urls_crawled,
                                      crawler_status=crawler_status)

            elif tag == 999:  # Error message
                error_msg = comm.recv(source=source, tag=tag)
                logging.error(f"Error from node {source}: {error_msg}")

                # Update crawler status
                if source < size - 1:  # It's a crawler
                    crawler_status[source] = 'error'
                    active_crawlers -= 1
                    idle_crawlers += 1
                    urls_errored += 1

                # Report error
                if redis_client:
                    send_status_update(redis_client, 'master', 0, 'Node error',
                                      error=True, error_msg=str(error_msg))

        # Assign work to idle crawlers if there are URLs in the queue
        if url_queue and idle_crawlers > 0:
            for i in range(1, size-1):
                if crawler_status[i] == 'idle':
                    if url_queue:
                        url = url_queue.popleft()
                        comm.send(url, dest=i, tag=1)  # Tag 1 for URL to crawl
                        crawler_status[i] = 'crawling'
                        active_crawlers += 1
                        idle_crawlers -= 1

                        # Report assignment
                        logging.info(f"Assigned URL {url} to crawler {i}")
                        if redis_client:
                            send_status_update(redis_client, 'master', 0, 'URL assigned',
                                              queue_size=len(url_queue),
                                              assigned_url=url,
                                              assigned_to=i)
                    else:
                        break

        # Short sleep to avoid busy waiting
        time.sleep(0.1)

    logging.info("Master node shutting down")

if __name__ == '__main__':
    master_process()