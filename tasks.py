#!/usr/bin/env python3
# filepath: tasks.py

"""
Core crawler and indexer tasks for the distributed web crawler.
All crawl data is stored in S3.
"""

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
from requests_aws4auth import AWS4Auth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import configuration from distributed_config
try:
    from distributed_config import (
        ELASTICSEARCH_URL, NODE_TYPE, OPENSEARCH_ENDPOINT,
        AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    )
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

# Import S3 storage - required
from s3_storage import save_to_s3, check_content_exists
USE_S3_STORAGE = True
logger.info("Using S3 for content storage")

@app.task(bind=True, name='tasks.crawl')
def crawl(self, url, depth=0, config=None):
    """Crawler task that fetches web pages"""
    if not config:
        config = {'request_delay': 1.0, 'timeout': 10, 'max_depth': 3, 'output_dir': 'output'}

    logger.info(f"Crawler {self.request.id} processing URL: {url} (depth {depth})")

    # Check if URL was already processed using S3
    if check_content_exists(url):
        logger.info(f"URL already processed in S3, skipping: {url}")
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


        # Prepare structured content for Elasticsearch and S3
        content = {
            'url': url,
            'title': title,
            'description': description,
            'html': response.text,
            'text_content': text_content,
            'crawl_timestamp': int(time.time()),
            'depth': depth
        }

        s3_key = save_to_s3(content, url)

        if s3_key:
            # Create minimal message for SQS (avoids size limits)
            indexer_message = {
                'url': url,
                's3_key': s3_key,  # Only send the S3 key, not full content
                'depth': depth
            }
            index.delay(indexer_message, url, config)

        else:
            logger.error(f"Failed to save content to S3 for {url}")


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
def index(content_indexer, url, config):
    """Indexer task that processes and indexes web content using OpenSearch and S3"""
    logger.info(f"Indexer processing content from: {url}")

    success = False
    error_message = None

    try:
        # First store content in S3 - this is the primary storage
        s3_key = content_indexer['s3_key']
        logger.info(f"Retrieving content from S3 key: {s3_key}")

        # Retrieve content from S3
        from s3_storage import retrieve_from_s3
        content = retrieve_from_s3(key=s3_key)

        if not content:
            raise Exception(f"Failed to retrieve content from S3 key: {s3_key}")

        logger.info(f"Successfully retrieved content from S3 for {url}")

        # Connect to AWS OpenSearch if configured
        if USE_AWS_OPENSEARCH:
            es_host = ELASTICSEARCH_URL
            logger.info(f"Using OpenSearch at {es_host}")

            # Determine which authentication method to use
            auth_method = "aws4auth"  # Default to AWS4Auth
            try:
                # Try to read from helper file if it exists
                with open("opensearch_auth_method.txt", "r") as f:
                    auth_method = f.read().strip()
            except (FileNotFoundError, IOError):
                pass

            if auth_method == "aws4auth":
                # Use AWS4Auth for OpenSearch
                aws_auth = AWS4Auth(
                    AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY,
                    AWS_REGION,
                    'es'
                )
                es = Elasticsearch(
                    hosts=[es_host],
                    http_auth=aws_auth,
                    use_ssl=es_host.startswith('https'),
                    verify_certs=True,
                    connection_class=RequestsHttpConnection
                )
            else:
                # Fall back to basic auth
                try:
                    from distributed_config import OPENSEARCH_USER, OPENSEARCH_PASS
                    es_user = OPENSEARCH_USER
                    es_pass = OPENSEARCH_PASS
                except ImportError:
                    es_user = config.get('elasticsearch_user', 'elastic')
                    es_pass = config.get('elasticsearch_password', 'elastic')

                es = Elasticsearch(
                    hosts=[es_host],
                    http_auth=(es_user, es_pass),
                    use_ssl=es_host.startswith('https'),
                    verify_certs=True,
                    connection_class=RequestsHttpConnection
                )

            # Create index if it doesn't exist
            index_name = config.get('elasticsearch_index', 'webcrawler')

            if not es.indices.exists(index=index_name):
                # Define mapping for better text search - ES 7.1 compatible
                mapping = {
                    "mappings": {
                        "properties": {
                            "url": {"type": "keyword"},  # Exact matches for URLs
                            "title": {"type": "text", "analyzer": "standard"},  # Full text search
                            "description": {"type": "text", "analyzer": "standard"},
                            "text_content": {"type": "text", "analyzer": "standard"},
                            "crawl_timestamp": {"type": "date", "format": "epoch_second"},
                            "depth": {"type": "integer"},
                            "s3_key": {"type": "keyword"}  # Store S3 key for reference
                        }
                    }
                }
                es.indices.create(index=index_name, body=mapping)
                logger.info(f"Created search index: {index_name}")

            # Add S3 key to content for indexing
            content_for_index = content.copy()
            content_for_index['s3_key'] = s3_key

            # Index the document - ES 7.1 compatible
            doc_id = hashlib.md5(url.encode()).hexdigest()
            es.index(index=index_name, id=doc_id, body=content_for_index)
            logger.info(f"Indexed content in OpenSearch: {url}")
        else:
            logger.info("OpenSearch not configured, content saved to S3 only")

        return {
            'status': 'success',
            'url': url,
            's3_key': s3_key,
            'index': index_name if USE_AWS_OPENSEARCH else None,
            'doc_id': doc_id if USE_AWS_OPENSEARCH else None
        }

    except Exception as e:
        error_message = str(e)
        logger.error(f"Error indexing content from {url}: {e}")
        return {'status': 'error', 'url': url, 'error': error_message}