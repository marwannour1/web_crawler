from elasticsearch import Elasticsearch
import hashlib
import logging

def search_content(query, config_file='crawler_config.json'):
    """Search indexed content using Elasticsearch"""
    from crawler_config import CrawlerConfig

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = CrawlerConfig(config_file).get_config()

        # Connect to Elasticsearch
        es_host = config.get('elasticsearch_url', 'http://localhost:9200')
        es_user = config.get('elasticsearch_user', 'elastic')
        es_pass = config.get('elasticsearch_password', 'elastic')
        index_name = config.get('elasticsearch_index', 'webcrawler')

        # Create ES connection with authentication
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

    except Exception as e:
        logger.error(f"Error searching: {e}")
        # For file fallback, you could search in actual files if needed
        return []