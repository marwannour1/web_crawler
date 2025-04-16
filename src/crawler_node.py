#!/usr/bin/env python3
from mpi4py import MPI
import logging
import time
import sys
import requests
from bs4 import BeautifulSoup
import urllib.parse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def crawler_process(config=None):
    """Crawler node that fetches web pages"""
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # This process should only run on crawler nodes
    if config:
        if rank < 1 or rank > config['num_crawlers']:
            return
    else:
        # Default behavior without config
        if rank < 1 or rank > 2:
            return

    logging.info(f"Crawler {rank} starting")

    # Default configuration if not provided
    if config is None:
        config = {'request_delay': 1.0, 'timeout': 10}

    # Main crawler loop
    while True:
        # Wait for a URL from master
        status = MPI.Status()
        data = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # Check if we should terminate
        if tag == 10:  # Termination signal
            logging.info(f"Crawler {rank} received termination signal")
            break

        # Extract URL and depth info
        url = data['url'] if isinstance(data, dict) else data   # it should always be a dictionary of url : depth
        depth = data.get('depth', 0) if isinstance(data, dict) else 0
        logging.info(f"Crawler {rank} received URL: {url} (depth {depth})")

        try:
            # Add delay to prevent overloading servers
            time.sleep(config['request_delay'])

            # Fetch the webpage content
            logging.info(f"Crawler {rank} fetching URL: {url}")
            headers = {'User-Agent': 'DistributedWebCrawler/1.0'}
            response = requests.get(url, headers=headers, timeout=config['timeout'])
            response.raise_for_status()  # Raise exception for 4XX/5XX responses

            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract links
            new_urls = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Normalize URL (handle relative URLs)
                full_url = urllib.parse.urljoin(url, href)
                # Filter out fragments and query parameters to standardize URL
                parts = urllib.parse.urlparse(full_url)
                clean_url = urllib.parse.urlunparse((parts.scheme, parts.netloc, parts.path, '', '', '')) # so we don't have duplicate crawling
                if clean_url:
                    new_urls.append(clean_url)

            # Extract title and meta description for content
            title = soup.title.string if soup.title else "No Title"
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            description = meta_desc['content'] if meta_desc and meta_desc.get('content') else "No description"

            # Extract text content (simplified)
            text_content = soup.get_text()[:1000]  # First 1000 chars for demo purposes

            # Prepare result
            content = f"URL: {url}\nTitle: {title}\nDescription: {description}\n\n{text_content}"

            # Send results back to master
            result = {
                'url': url,
                'new_urls': new_urls,
                'content': content,
                'depth': depth
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