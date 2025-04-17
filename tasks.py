import time
import requests
from urllib.parse import urljoin, urlparse, urlunparse
from bs4 import BeautifulSoup
import hashlib
import os
import logging
from celery_app import app

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.task(bind=True, name='tasks.crawl')
def crawl(self, url, depth=0, config=None):
    """Crawler task that fetches web pages"""
    if not config:
        config = {'request_delay': 1.0, 'timeout': 10, 'max_depth': 3, 'output_dir': 'output'}

    logger.info(f"Crawler {self.request.id} processing URL: {url} (depth {depth})")

    # Create a unique filename for this URL to check if it was already processed
    url_hash = hashlib.md5(url.encode()).hexdigest()
    output_dir = config['output_dir']
    check_path = os.path.join(output_dir, f"indexer_worker/{url_hash}.txt")

    # If the file exists, this URL was already crawled
    if os.path.exists(check_path):
        logger.info(f"URL already processed, skipping: {url}")
        return {'status': 'skipped', 'url': url, 'reason': 'already_processed'}

    try:
        # Add delay to prevent overloading servers
        time.sleep(config['request_delay'])

        # Fetch the webpage content
        headers = {'User-Agent': 'DistributedWebCrawler/1.0'}
        response = requests.get(url, headers=headers, timeout=config['timeout'])
        response.raise_for_status()

        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract links
        new_urls = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Normalize URL
            full_url = urljoin(url, href)
            # Filter out fragments and query parameters
            parts = urlparse(full_url)
            clean_url = urlunparse((parts.scheme, parts.netloc, parts.path, '', '', ''))
            if clean_url:
                new_urls.append(clean_url)

        # Extract title and meta description
        title = soup.title.string if soup.title else "No Title"
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        description = meta_desc['content'] if meta_desc and meta_desc.get('content') else "No description"

        # Extract text content
        text_content = soup.get_text()[:1000]  # First 1000 chars

        # Prepare content
        content = f"URL: {url}\nTitle: {title}\nDescription: {description}\n\n{text_content}"

        # Schedule indexing task
        index.delay(content, url, config)

        # Schedule crawling of new URLs if not at max depth
        if depth < config['max_depth']:
            for new_url in new_urls:
                # Check domain restrictions if any
                allowed = True
                if config.get('restricted_domains'):
                    allowed = not any(domain in new_url for domain in config['restricted_domains'])

                if allowed:
                    # Send to crawl queue with incremented depth
                    crawl.delay(new_url, depth + 1, config)

        logger.info(f"Crawler completed processing URL: {url}")
        return {'status': 'success', 'url': url, 'new_urls_count': len(new_urls)}

    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}")
        return {'status': 'error', 'url': url, 'error': str(e)}

@app.task(name='tasks.index')
def index(content, url, config):
    """Indexer task that processes and indexes web content"""
    logger.info(f"Indexer processing content from: {url}")

    try:
        # Setup output directory
        output_dir = config['output_dir']
        os.makedirs(output_dir, exist_ok=True)

        # Create a subdirectory for this worker
        worker_id = os.environ.get('CELERY_WORKER_ID', 'worker')
        index_dir = os.path.join(output_dir, f"indexer_{worker_id}")
        os.makedirs(index_dir, exist_ok=True)

        # Generate filename based on URL
        filename = hashlib.md5(url.encode()).hexdigest() + ".txt"
        filepath = os.path.join(index_dir, filename)

        # Save content to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"Saved content to {filepath}")
        return {'status': 'success', 'url': url, 'file': filepath}

    except Exception as e:
        logger.error(f"Error indexing content from {url}: {e}")
        return {'status': 'error', 'url': url, 'error': str(e)}