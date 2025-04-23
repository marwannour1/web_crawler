#!/usr/bin/env python3
# filepath: search.py

"""
Search interface for the web crawler using Amazon OpenSearch Service.
Provides functionality to search indexed content with fallback to S3.
"""

from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection  # Correct import for RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests
import hashlib
import logging
import os
import json
import argparse
import boto3
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def search_content(query, config_file='crawler_config.json'):
    """Search indexed content using OpenSearch with AWS auth"""
    from crawler_config import CrawlerConfig

    try:
        # Try to use distributed config if available
        try:
            from distributed_config import (
                ELASTICSEARCH_URL, OPENSEARCH_ENDPOINT,
                AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                OPENSEARCH_USER, OPENSEARCH_PASS
            )
            es_host = ELASTICSEARCH_URL
            use_aws = bool(OPENSEARCH_ENDPOINT)
            logger.info(f"Using distributed search at {es_host}")
            distributed_mode = True
        except ImportError:
            # Load from standard config
            distributed_mode = False
            use_aws = False
            logger.info("Using local configuration")

        # Load configuration
        config = CrawlerConfig(config_file).get_config()

        # Connect to Elasticsearch or OpenSearch
        if not distributed_mode:
            es_host = config.get('elasticsearch_url', 'http://localhost:9200')
            es_user = config.get('elasticsearch_user', 'elastic')
            es_pass = config.get('elasticsearch_password', 'elastic')
            use_aws = False

        index_name = config.get('elasticsearch_index', 'webcrawler')
        logger.info(f"Connecting to search service at {es_host}")

        # Create ES connection with authentication
        if use_aws:
            # Determine authentication method from file if available
            auth_method = "aws4auth"  # Default
            try:
                with open("opensearch_auth_method.txt", "r") as f:
                    auth_method = f.read().strip()
            except (FileNotFoundError, IOError):
                pass

            if auth_method == "aws4auth":
                # AWS OpenSearch connection with IAM auth
                aws_auth = AWS4Auth(
                    AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY,
                    AWS_REGION,
                    'es'  # Service name for OpenSearch
                )
                es = Elasticsearch(
                    hosts=[es_host],
                    http_auth=aws_auth,
                    use_ssl=es_host.startswith('https'),
                    verify_certs=True,
                    connection_class=RequestsHttpConnection
                )
            else:
                # Basic auth fallback
                es = Elasticsearch(
                    hosts=[es_host],
                    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
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

        # Build search query
        search_query = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^2", "description^1.5", "text_content"],
                    "type": "best_fields"
                }
            },
            "highlight": {
                "fields": {
                    "title": {},
                    "description": {},
                    "text_content": {"fragment_size": 150, "number_of_fragments": 3}
                }
            },
            "_source": ["url", "title", "description", "crawl_timestamp", "s3_key"],
            "size": 10
        }

        # Check if index exists
        if not es.indices.exists(index=index_name):
            logger.warning(f"Index {index_name} does not exist yet. No content has been indexed.")
            # Try S3 fallback
            return search_s3(query, config)

        # Execute search
        logger.info(f"Executing search for '{query}' in index '{index_name}'")
        response = es.search(index=index_name, body=search_query)

        # Format results
        results = []
        for hit in response["hits"]["hits"]:
            result = {
                "score": hit["_score"],
                "url": hit["_source"]["url"],
                "title": hit["_source"]["title"],
                "description": hit["_source"]["description"],
                "s3_key": hit["_source"].get("s3_key", ""),
                "highlights": hit.get("highlight", {})
            }
            results.append(result)

        logger.info(f"Found {len(results)} results")
        return results

    except Exception as e:
        logger.error(f"Error searching OpenSearch: {e}")

        # Try S3 fallback first
        try:
            results = search_s3(query, config)
            if results:
                logger.info(f"Found {len(results)} results in S3")
                return results
        except Exception as s3_error:
            logger.error(f"S3 search failed: {s3_error}")

        # File fallback as last resort
        try:
            results = search_files(query, config.get('output_dir', 'output'))
            return results
        except Exception as file_error:
            logger.error(f"File fallback search failed: {file_error}")

        return []

def search_s3(query, config):
    """Search content in S3 bucket"""
    from aws_config import S3_BUCKET_NAME, S3_OUTPUT_PREFIX
    from aws_config import ensure_aws_clients, s3_client

    logger.info(f"Searching in S3 bucket: {S3_BUCKET_NAME}")

    ensure_aws_clients()

    try:
        # List all JSON files in the output directory
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_OUTPUT_PREFIX,
        )

        if 'Contents' not in response:
            logger.warning(f"No content found in S3 bucket: {S3_BUCKET_NAME}")
            return []

        results = []
        query_terms = [term.lower() for term in query.split()]

        # Process only JSON files
        json_files = [item['Key'] for item in response['Contents']
                      if item['Key'].endswith('.json')]

        logger.info(f"Scanning {len(json_files)} files in S3")

        for key in json_files:
            try:
                # Get the file content
                obj = s3_client.get_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=key
                )
                content = json.loads(obj['Body'].read().decode('utf-8'))

                # Simple scoring - count term occurrences in text_content
                score = 0
                text = content.get('text_content', '').lower()
                title = content.get('title', '').lower()
                description = content.get('description', '').lower()

                for term in query_terms:
                    # Weight title and description higher
                    score += text.count(term)
                    score += title.count(term) * 3  # Title is 3x more important
                    score += description.count(term) * 2  # Description 2x more important

                if score > 0:
                    # Find highlights
                    highlights = []
                    sentences = text.split('.')

                    for sentence in sentences:
                        if any(term in sentence.lower() for term in query_terms):
                            highlights.append(sentence.strip())
                            if len(highlights) >= 3:  # Limit to 3 highlights
                                break

                    results.append({
                        "score": score,
                        "url": content.get('url', ''),
                        "title": content.get('title', 'Unknown Title'),
                        "description": content.get('description', ''),
                        "s3_key": key,
                        "highlights": {"text_content": highlights}
                    })

            except Exception as e:
                logger.error(f"Error processing S3 file {key}: {e}")

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)

        # Limit to 10 results
        return results[:10]

    except Exception as e:
        logger.error(f"Error searching S3: {e}")
        return []

def search_files(query, output_dir):
    """Fallback search function that searches in indexed files"""
    logger.info(f"Falling back to file-based search in directory: {output_dir}")

    results = []
    index_dir = os.path.join(output_dir, "indexer_worker")

    if not os.path.exists(index_dir):
        logger.warning(f"Index directory does not exist: {index_dir}")
        return results

    query_terms = [term.lower() for term in query.split()]

    for filename in os.listdir(index_dir):
        if not filename.endswith(".txt"):
            continue

        filepath = os.path.join(index_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

                # Simple scoring - count term occurrences
                score = 0
                for term in query_terms:
                    score += content.lower().count(term)

                if score > 0:
                    # Extract URL, title, and description
                    url_match = content.split('\n')[0] if content else ""
                    title_match = content.split('\n')[1] if len(content.split('\n')) > 1 else ""
                    desc_match = content.split('\n')[2] if len(content.split('\n')) > 2 else ""

                    url = url_match.replace("URL: ", "") if url_match.startswith("URL: ") else ""
                    title = title_match.replace("Title: ", "") if title_match.startswith("Title: ") else "Unknown Title"
                    description = desc_match.replace("Description: ", "") if desc_match.startswith("Description: ") else ""

                    # Find highlights
                    highlights = []
                    content_lines = content.split('\n')[3:]
                    content_text = '\n'.join(content_lines)

                    for line in content_text.split('.'):
                        if any(term in line.lower() for term in query_terms):
                            highlights.append(line.strip())

                    results.append({
                        "score": score,
                        "url": url,
                        "title": title,
                        "description": description,
                        "highlights": {"text_content": highlights[:3]}  # Limit to 3 highlights
                    })
        except Exception as e:
            logger.error(f"Error processing file {filename}: {e}")

    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)

    # Limit to 10 results
    return results[:10]

def main():
    """Command-line interface for search function"""
    parser = argparse.ArgumentParser(description="Search indexed content")
    parser.add_argument("query", help="Search query")
    parser.add_argument("--config", default="crawler_config.json", help="Path to config file")
    parser.add_argument("--output-format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--source", choices=["opensearch", "s3", "file", "all"], default="all",
                       help="Search source (opensearch, s3, file, or all)")

    args = parser.parse_args()

    # Choose search function based on source
    if args.source == "opensearch":
        results = search_content(args.query, args.config)
    elif args.source == "s3":
        from crawler_config import CrawlerConfig
        config = CrawlerConfig(args.config).get_config()
        results = search_s3(args.query, config)
    elif args.source == "file":
        from crawler_config import CrawlerConfig
        config = CrawlerConfig(args.config).get_config()
        results = search_files(args.query, config.get('output_dir', 'output'))
    else:
        # Default: try all sources in order
        results = search_content(args.query, args.config)

    if args.output_format == "json":
        print(json.dumps(results, indent=2))
    else:
        print(f"Found {len(results)} results for '{args.query}':\n")

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']} (Score: {result['score']:.2f})")
            print(f"   URL: {result['url']}")
            if 's3_key' in result and result['s3_key']:
                print(f"   S3: {result['s3_key']}")
            print(f"   Description: {result['description'][:100]}...")

            if "text_content" in result["highlights"]:
                print("   Highlights:")
                for fragment in result["highlights"]["text_content"]:
                    print(f"   - ...{fragment}...")
            print()

if __name__ == "__main__":
    main()