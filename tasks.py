import time
import requests
from urllib.parse import urljoin, urlparse, urlunparse
from bs4 import BeautifulSoup
import hashlib
import os
import logging
from celery_app import app
import json
from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


try:
    from distributed_config import ELASTICSEARCH_URL, NODE_TYPE, OPENSEARCH_ENDPOINT
    DISTRIBUTED_MODE = True
    USE_AWS_OPENSEARCH = bool(OPENSEARCH_ENDPOINT)
    logger.info(f"Running in distributed mode as {NODE_TYPE} node")
    if USE_AWS_OPENSEARCH:
        logger.info(f"Using AWS OpenSearch Service at {OPENSEARCH_ENDPOINT}")
except ImportError:
    DISTRIBUTED_MODE = False
    USE_AWS_OPENSEARCH = False
    NODE_TYPE = "local"
    logger.info("Running in local mode")

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
        text_content = soup.get_text()

        # Prepare structured content for Elasticsearch
        content = {
            'url': url,
            'title': title,
            'description': description,
            'text_content': text_content,
            'html': response.text,  # Include raw HTML for Elasticsearch to process
            'crawl_timestamp': time.time(),
            'depth': depth
        }
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
    """Indexer task that processes and indexes web content using Elasticsearch/OpenSearch"""
    logger.info(f"Indexer processing content from: {url}")

    success = False
    error_message = None

    try:
        # Connect to Elasticsearch or AWS OpenSearch
        if DISTRIBUTED_MODE:
            es_host = ELASTICSEARCH_URL
            logger.info(f"Using distributed search service at {es_host}")

            # Get credentials from distributed config if available
            try:
                from distributed_config import OPENSEARCH_USER, OPENSEARCH_PASS
                es_user = OPENSEARCH_USER
                es_pass = OPENSEARCH_PASS
            except ImportError:
                es_user = config.get('elasticsearch_user', 'elastic')
                es_pass = config.get('elasticsearch_password', 'elastic')
        else:
            es_host = config.get('elasticsearch_url', 'http://localhost:9200')
            es_user = config.get('elasticsearch_user', 'elastic')
            es_pass = config.get('elasticsearch_password', 'elastic')

        # Create the connection with authentication
        if USE_AWS_OPENSEARCH:
            # AWS OpenSearch connection
            es = Elasticsearch(
                hosts=[es_host],
                http_auth=(es_user, es_pass),
                use_ssl=es_host.startswith('https'),
                verify_certs=True,
                connection_class=RequestsHttpConnection
            )
        else:
            # Standard Elasticsearch connection
            es = Elasticsearch(
                [es_host],
                http_auth=(es_user, es_pass)
            )

        # Create index if it doesn't exist
        index_name = config.get('elasticsearch_index', 'webcrawler')

        if not es.indices.exists(index=index_name):
            # Define mapping for better text search
            mapping = {
                "mappings": {
                    "properties": {
                        "url": {"type": "keyword"},  # Exact matches for URLs
                        "title": {"type": "text", "analyzer": "standard"},  # Full text search
                        "description": {"type": "text", "analyzer": "standard"},
                        "text_content": {"type": "text", "analyzer": "standard"},
                        "crawl_timestamp": {"type": "date", "format": "epoch_second"},
                        "depth": {"type": "integer"}
                    }
                }
            }
            es.indices.create(index=index_name, body=mapping)
            logger.info(f"Created search index: {index_name}")

        # Index the document
        doc_id = hashlib.md5(url.encode()).hexdigest()
        es.index(index=index_name, id=doc_id, body=content)

        logger.info(f"Indexed content in search service: {url}")
        success = True

        # For backward compatibility, also save to file if needed
        if config.get('save_to_file', True) and (not DISTRIBUTED_MODE or NODE_TYPE == 'indexer'):
            # Setup output directory
            output_dir = config['output_dir']
            os.makedirs(output_dir, exist_ok=True)

            # Create a subdirectory for files
            index_dir = os.path.join(output_dir, "indexer_worker")
            os.makedirs(index_dir, exist_ok=True)

            # Generate filename based on URL
            filename = hashlib.md5(url.encode()).hexdigest() + ".txt"
            filepath = os.path.join(index_dir, filename)

            # Format content as text for file storage
            formatted_content = f"URL: {content['url']}\nTitle: {content['title']}\nDescription: {content['description']}\n\n{content['text_content']}"

            # Save content to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(formatted_content)

            logger.info(f"Also saved content to file: {filepath}")

        return {
            'status': 'success',
            'url': url,
            'index': index_name,
            'doc_id': doc_id
        }

    except Exception as e:
        logger.error(f"Error indexing content from {url}: {e}")
        error_message = str(e)

        # Try to save to file anyway if Elasticsearch fails
        try:
            if config.get('save_to_file', True):
                output_dir = config['output_dir']
                os.makedirs(output_dir, exist_ok=True)

                index_dir = os.path.join(output_dir, "indexer_worker")
                os.makedirs(index_dir, exist_ok=True)

                filename = hashlib.md5(url.encode()).hexdigest() + ".txt"
                filepath = os.path.join(index_dir, filename)

                formatted_content = f"URL: {content['url']}\nTitle: {content['title']}\nDescription: {content['description']}\n\n{content['text_content']}"

                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(formatted_content)

                logger.info(f"Saved content to file despite indexing error: {filepath}")
        except Exception as file_error:
            logger.error(f"Error saving to file: {file_error}")

        return {'status': 'error', 'url': url, 'error': error_message}