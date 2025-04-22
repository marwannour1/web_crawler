#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection
import hashlib
import logging
import os
import json
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def search_content(query, config_file='crawler_config.json'):
    """Search indexed content using Elasticsearch or OpenSearch"""
    from crawler_config import CrawlerConfig

    try:
        # Try to use distributed config if available
        try:
            from distributed_config import ELASTICSEARCH_URL, OPENSEARCH_ENDPOINT, OPENSEARCH_USER, OPENSEARCH_PASS
            es_host = ELASTICSEARCH_URL
            es_user = OPENSEARCH_USER
            es_pass = OPENSEARCH_PASS
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
            "_source": ["url", "title", "description", "crawl_timestamp"],
            "size": 10
        }

        # Check if index exists
        if not es.indices.exists(index=index_name):
            logger.warning(f"Index {index_name} does not exist yet. No content has been indexed.")
            return []

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
                "highlights": hit.get("highlight", {})
            }
            results.append(result)

        logger.info(f"Found {len(results)} results")
        return results

    except Exception as e:
        logger.error(f"Error searching: {e}")

        # File fallback: search in files if Elasticsearch fails
        try:
            results = search_files(query, config.get('output_dir', 'output'))
            return results
        except Exception as file_error:
            logger.error(f"File fallback search failed: {file_error}")

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

    args = parser.parse_args()

    results = search_content(args.query, args.config)

    if args.output_format == "json":
        print(json.dumps(results, indent=2))
    else:
        print(f"Found {len(results)} results for '{args.query}':\n")

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']} (Score: {result['score']:.2f})")
            print(f"   URL: {result['url']}")
            print(f"   Description: {result['description'][:100]}...")

            if "text_content" in result["highlights"]:
                print("   Highlights:")
                for fragment in result["highlights"]["text_content"]:
                    print(f"   - ...{fragment}...")
            print()

if __name__ == "__main__":
    main()