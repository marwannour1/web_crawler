#!/usr/bin/env python3
# filepath: g:\ain_shams\courses\Distributed Computing CSE354\projects\web_crawler\search.py

import argparse
import json
from elasticsearch import Elasticsearch
from crawler_config import CrawlerConfig

def search_content(query, config_file='crawler_config.json'):
    """Search indexed content using Elasticsearch"""

    # Load configuration
    config = CrawlerConfig(config_file).get_config()

    # Connect to Elasticsearch
    es_host = config.get('elasticsearch_url', 'http://localhost:9200')
    index_name = config.get('elasticsearch_index', 'webcrawler')

    es = Elasticsearch([es_host])

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

    # Execute search
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

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search web crawler content')
    parser.add_argument('query', help='Search query')
    parser.add_argument('--config', default='crawler_config.json', help='Path to config file')

    args = parser.parse_args()

    results = search_content(args.query, args.config)

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