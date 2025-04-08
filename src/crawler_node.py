#!/usr/bin/env python3
from mpi4py import MPI
import logging
import time
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def crawler_process():
    """Crawler node that fetches web pages"""
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # This process should only run on crawler nodes (ranks 1 and 2 in our example)
    if rank < 1 or rank > 2:
        return

    logging.info(f"Crawler {rank} starting")

    # Main crawler loop
    while True:
        # Wait for a URL from master
        status = MPI.Status()
        url = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # Check if we should terminate
        if tag == 10:  # Termination signal
            logging.info(f"Crawler {rank} received termination signal")
            break

        logging.info(f"Crawler {rank} received URL: {url}")

        try:
            # Simulate crawling process
            logging.info(f"Crawler {rank} processing URL: {url}")
            time.sleep(2)  # Simulate network and processing time

            # In a real implementation:
            # 1. Fetch the webpage content using requests
            # 2. Parse HTML using BeautifulSoup
            # 3. Extract new URLs according to depth and domain restrictions
            # 4. Extract content for indexing

            # Simulate discovering new URLs
            new_urls = [f"{url}/page{i}" for i in range(1, 4)]

            # Simulate extracting content
            content = f"This is the simulated content from {url}. It contains some text for indexing."

            # Send results back to master
            result = {
                'url': url,
                'new_urls': new_urls,
                'content': content
            }
            comm.send(result, dest=0, tag=1)  # Tag 1 for crawler results
            logging.info(f"Crawler {rank} completed processing URL: {url}")

        except Exception as e:
            # Report any errors to master
            logging.error(f"Crawler {rank} error processing URL {url}: {e}")
            comm.send(f"Error crawling {url}: {str(e)}", dest=0, tag=999)

    logging.info(f"Crawler {rank} shutting down")

if __name__ == '__main__':
    crawler_process()