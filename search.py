#!/usr/bin/env python3
# filepath: search.py

"""
Enhanced search interface for the distributed web crawler using Amazon OpenSearch Service.
Provides a user-friendly interface to search indexed content with highlighting and advanced options.
"""

import difflib
from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests
import hashlib
import logging
import os
import json
import argparse
import boto3
import time
import sys
from botocore.exceptions import ClientError
from datetime import datetime
import textwrap
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ANSI color codes for terminal styling
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    HIGHLIGHT = '\033[43m\033[30m'  # Yellow background with black text

def print_header(title):
    """Print a formatted header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{title.center(60)}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.ENDC}\n")

def print_result(i, result, show_highlights=True, max_highlight_len=150):
    """Print a single search result with clean, readable formatting"""
    # 1. Title and score
    print(f"{Colors.BOLD}{i}. {Colors.GREEN}{result['title']}{Colors.ENDC}")

    # 2. URL (most important identifier)
    print(f"   {Colors.UNDERLINE}{result['url']}{Colors.ENDC}")

    # 3. Description (clean format)
    description = result['description']
    if len(description) > 100:
        description = description[:97] + "..."
    print(f"   {description}")

    # 4. Cleaner highlights (if available)
    if show_highlights and "highlights" in result:
        highlights = []

        # Process text content highlights
        if "text_content" in result["highlights"] and result["highlights"]["text_content"]:
            for fragment in result["highlights"]["text_content"]:
                # Clean up the fragment
                clean = clean_highlight(fragment)
                if clean and len(clean.strip()) > 10:  # Only show meaningful highlights
                    highlights.append(clean)

        # Process title highlights
        if "title" in result["highlights"] and result["highlights"]["title"]:
            clean = clean_highlight(result["highlights"]["title"][0])
            if clean:
                highlights.append(f"Title match: {clean}")

        # Display cleaned highlights (max 2)
        if highlights:
            print(f"\n   {Colors.BOLD}Key matches:{Colors.ENDC}")
            for idx, highlight in enumerate(highlights[:2]):  # Limit to 2 most relevant
                print(f"   • {highlight}")

    print()  # Add space between results

def clean_highlight(text):
    """Clean up highlight fragments to make them readable"""
    # Replace HTML tags with proper formatting
    text = text.replace('<em>', f"{Colors.BOLD}")
    text = text.replace('</em>', f"{Colors.ENDC}")

    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)

    # Remove HTML fragments
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up any markdown-style markers
    text = re.sub(r'\*\*', '', text)

    return text.strip()

def search_files(query, output_dir='output', show_progress=True):
    """
    AWS-optimized version - redirects to S3 search instead of local files
    Local file search isn't useful for AWS deployments
    """
    if show_progress:
        print(f"{Colors.CYAN}Local file search not supported in AWS deployment, using S3 search instead...{Colors.ENDC}")

    # Get configuration and redirect to S3 search
    from crawler_config import CrawlerConfig
    config = CrawlerConfig().get_config()
    return search_s3(query, config, show_progress)

def search_content(query, config_file='crawler_config.json', show_progress=True, advanced=False):
    """Search indexed content using OpenSearch with improved formatting"""
    from crawler_config import CrawlerConfig

    if show_progress:
        print(f"\n{Colors.BOLD}Searching for: {Colors.GREEN}{query}{Colors.ENDC}")
        print(f"{Colors.CYAN}Checking OpenSearch service...{Colors.ENDC}")

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
            if show_progress:
                print(f"{Colors.CYAN}Using AWS OpenSearch service at {es_host}{Colors.ENDC}")
            distributed_mode = True
        except ImportError:
            # Load from standard config
            distributed_mode = False
            use_aws = False
            if show_progress:
                print(f"{Colors.CYAN}Using local configuration{Colors.ENDC}")

        # Load configuration
        config = CrawlerConfig(config_file).get_config()

        # Connect to Elasticsearch or OpenSearch
        if not distributed_mode:
            es_host = config.get('elasticsearch_url', 'http://localhost:9200')
            es_user = config.get('elasticsearch_user', 'elastic')
            es_pass = config.get('elasticsearch_password', 'elastic')
            use_aws = False

        index_name = config.get('elasticsearch_index', 'webcrawler')

        if show_progress:
            print(f"{Colors.CYAN}Connecting to search index: {Colors.BOLD}{index_name}{Colors.ENDC}")

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

        # Build search query for advanced or standard search
        if advanced:
            search_query = {
                "query": {
                    "query_string": {
                        "query": query,
                        "fields": ["title^3", "description^2", "text_content"],
                        "default_operator": "AND"
                    }
                },
                "highlight": {
                    "fields": {
                        "title": {"number_of_fragments": 1},
                        "description": {"number_of_fragments": 1},
                        "text_content": {"fragment_size": 150, "number_of_fragments": 3}
                    },
                    "pre_tags": ["**"],  # Markdown-style highlighting
                    "post_tags": ["**"]
                },
                "_source": ["url", "title", "description", "crawl_timestamp", "s3_key"],
                "size": 15,
                "sort": [
                    "_score",  # Primary sort by relevance score
                    {"crawl_timestamp": {"order": "desc"}}  # Secondary sort by date
                ]
            }
        else:
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
            if show_progress:
                print(f"\n{Colors.WARNING}⚠️ Index {index_name} does not exist yet. No content has been indexed.{Colors.ENDC}")
                print(f"{Colors.CYAN}Trying S3 fallback search...{Colors.ENDC}")
            # Try S3 fallback
            return search_s3(query, config, show_progress)

        # Execute search with timing
        start_time = time.time()
        if show_progress:
            print(f"{Colors.CYAN}Executing search...{Colors.ENDC}")

        response = es.search(index=index_name, body=search_query)
        search_time = time.time() - start_time

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
            if "crawl_timestamp" in hit["_source"]:
                result["date"] = datetime.fromtimestamp(hit["_source"]["crawl_timestamp"]).strftime('%Y-%m-%d %H:%M')
            results.append(result)

        if show_progress:
            print(f"{Colors.GREEN}Search completed in {search_time:.2f} seconds{Colors.ENDC}")
            print(f"{Colors.BOLD}Found {len(results)} results in OpenSearch{Colors.ENDC}")
        return results

    except Exception as e:
        if show_progress:
            print(f"{Colors.RED}Error searching OpenSearch: {e}{Colors.ENDC}")
            print(f"{Colors.CYAN}Trying S3 fallback search...{Colors.ENDC}")

        # Try S3 fallback
        try:
            results = search_s3(query, config, show_progress)
            if results:
                return results
        except Exception as s3_error:
            if show_progress:
                print(f"{Colors.RED}S3 search failed: {s3_error}{Colors.ENDC}")
                print(f"{Colors.CYAN}Trying local file search...{Colors.ENDC}")

        # File fallback as last resort
        try:
            results = search_files(query, config.get('output_dir', 'output'), show_progress)
            return results
        except Exception as file_error:
            if show_progress:
                print(f"{Colors.RED}All search methods failed{Colors.ENDC}")
            return []

def search_s3(query, config, show_progress=True):
    """Search content in S3 bucket with clean, readable results"""
    from aws_config import S3_BUCKET_NAME, S3_OUTPUT_PREFIX
    from aws_config import ensure_aws_clients, s3_client

    if show_progress:
        print(f"{Colors.CYAN}Searching in S3 bucket: {S3_BUCKET_NAME}{Colors.ENDC}")

    ensure_aws_clients()
    start_time = time.time()

    try:
        # List all JSON files in the output directory
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=S3_OUTPUT_PREFIX,
        )

        if 'Contents' not in response:
            if show_progress:
                print(f"{Colors.WARNING}⚠️ No content found in S3 bucket{Colors.ENDC}")
            return []

        results = []
        query_terms = [term.lower() for term in query.split()]

        # Process only JSON files
        json_files = [item['Key'] for item in response['Contents']
                    if item['Key'].endswith('.json')]

        if show_progress:
            print(f"{Colors.CYAN}Scanning {len(json_files)} files in S3...{Colors.ENDC}")
            # Simple progress indicator
            total = len(json_files)
            progress_interval = max(1, total // 20)  # Update progress ~20 times

        # Process files in batches for efficiency
        for i, key in enumerate(json_files):
            try:
                # Show progress
                if show_progress and i % progress_interval == 0:
                    percent = (i / total) * 100
                    sys.stdout.write(f"\r{Colors.CYAN}Progress: {percent:.1f}% ({i}/{total}){Colors.ENDC}")
                    sys.stdout.flush()

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
                    # Extract meaningful sentences containing search terms
                    # Split text into sentences (handling common abbreviations)
                    text_content = content.get('text_content', '')
                    # Simple sentence splitting with regex
                    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', text_content)

                    # Find sentences with query terms
                    matching_sentences = []
                    for sentence in sentences:
                        sentence = sentence.strip()
                        # Only consider reasonably sized sentences
                        if 20 <= len(sentence) <= 300:
                            sentence_lower = sentence.lower()
                            for term in query_terms:
                                if term in sentence_lower:
                                    # Format the sentence to highlight the term
                                    highlighted = re.sub(
                                        r'(?i)\b(' + re.escape(term) + r')\b',
                                        f"<em>\\1</em>",
                                        sentence
                                    )
                                    matching_sentences.append(highlighted)
                                    break  # One match per sentence is enough

                    # Limit and deduplicate sentences
                    unique_sentences = []
                    for sentence in matching_sentences:
                        # Check if this sentence is too similar to ones we already have
                        is_duplicate = False
                        for existing in unique_sentences:
                            # Compare without HTML tags
                            clean_sentence = re.sub(r'<[^>]+>', '', sentence).lower()
                            clean_existing = re.sub(r'<[^>]+>', '', existing).lower()

                            # If 50% similarity, consider a duplicate
                            if len(clean_sentence) > 0 and len(clean_existing) > 0:
                                similarity = difflib.SequenceMatcher(None, clean_sentence, clean_existing).ratio()
                                if similarity > 0.5:
                                    is_duplicate = True
                                    break

                        if not is_duplicate:
                            unique_sentences.append(sentence)
                            if len(unique_sentences) >= 2:  # Limit to 2 unique sentences
                                break

                    # Create result entry
                    results.append({
                        "score": score,
                        "url": content.get('url', ''),
                        "title": content.get('title', 'Unknown Title'),
                        "description": content.get('description', ''),
                        "s3_key": key,
                        "highlights": {"text_content": unique_sentences},
                        "date": datetime.fromtimestamp(content.get('crawl_timestamp', 0)).strftime('%Y-%m-%d %H:%M')
                    })

            except Exception as e:
                logger.error(f"Error processing S3 file {key}: {e}")

        # Clear progress indicator
        if show_progress:
            sys.stdout.write("\r" + " " * 60 + "\r")
            sys.stdout.flush()

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)
        search_time = time.time() - start_time

        if show_progress:
            print(f"{Colors.GREEN}S3 search completed in {search_time:.2f} seconds{Colors.ENDC}")
            print(f"{Colors.BOLD}Found {len(results)} results in S3{Colors.ENDC}")

        # Return top results
        return results[:15]

    except Exception as e:
        if show_progress:
            print(f"{Colors.RED}Error searching S3: {e}{Colors.ENDC}")
        return []


def interactive_search():
    """Interactive search interface with pagination and options"""
    print_header("DISTRIBUTED WEB CRAWLER SEARCH")
    print(f"Type {Colors.BOLD}'exit'{Colors.ENDC} at any time to quit\n")

    # Get initial search query
    query = input(f"{Colors.BOLD}Enter search query: {Colors.ENDC}")
    if query.lower() == 'exit':
        return

    while True:
        # Execute search
        try:
            print(f"\n{Colors.CYAN}Searching for: {Colors.BOLD}{query}{Colors.ENDC}")
            results = search_content(query, show_progress=True)

            if not results:
                print(f"\n{Colors.WARNING}No results found.{Colors.ENDC}")
                query = input(f"\n{Colors.BOLD}Enter new search query (or 'exit'): {Colors.ENDC}")
                if query.lower() == 'exit':
                    break
                continue

            # Pagination variables
            page_size = 5
            current_page = 0
            total_pages = (len(results) + page_size - 1) // page_size

            while True:
                # Clear screen
                os.system('cls' if os.name == 'nt' else 'clear')

                # Display header
                print_header(f"SEARCH RESULTS FOR: {query}")

                # Display current page of results
                start_idx = current_page * page_size
                end_idx = min(start_idx + page_size, len(results))

                for i, result in enumerate(results[start_idx:end_idx], start=start_idx+1):
                    print_result(i, result)

                # Display pagination info and commands
                print(f"\n{Colors.BOLD}Page {current_page + 1} of {total_pages} | "
                      f"Displaying results {start_idx + 1}-{end_idx} of {len(results)}{Colors.ENDC}")

                print(f"\n{Colors.BOLD}Commands:{Colors.ENDC}")
                print(f"  {Colors.GREEN}n{Colors.ENDC} - Next page")
                print(f"  {Colors.GREEN}p{Colors.ENDC} - Previous page")
                print(f"  {Colors.GREEN}v [number]{Colors.ENDC} - View full content of result")
                print(f"  {Colors.GREEN}q{Colors.ENDC} - New search query")
                print(f"  {Colors.GREEN}exit{Colors.ENDC} - Exit search")

                command = input(f"\n{Colors.BOLD}Enter command: {Colors.ENDC}").strip().lower()

                if command == 'n':  # Next page
                    if current_page < total_pages - 1:
                        current_page += 1
                    else:
                        print(f"{Colors.WARNING}Already on last page{Colors.ENDC}")
                        input("Press Enter to continue...")

                elif command == 'p':  # Previous page
                    if current_page > 0:
                        current_page -= 1
                    else:
                        print(f"{Colors.WARNING}Already on first page{Colors.ENDC}")
                        input("Press Enter to continue...")

                elif command.startswith('v '):  # View full content
                    try:
                        idx = int(command.split()[1]) - 1
                        if 0 <= idx < len(results):
                            view_full_content(results[idx])
                            input("\nPress Enter to return to results...")
                        else:
                            print(f"{Colors.WARNING}Invalid result number{Colors.ENDC}")
                            input("Press Enter to continue...")
                    except (ValueError, IndexError):
                        print(f"{Colors.WARNING}Invalid command format{Colors.ENDC}")
                        input("Press Enter to continue...")

                elif command == 'q':  # New search query
                    break

                elif command == 'exit':
                    return

                else:
                    print(f"{Colors.WARNING}Unknown command: {command}{Colors.ENDC}")
                    input("Press Enter to continue...")

            if command == 'q':
                query = input(f"\n{Colors.BOLD}Enter new search query (or 'exit'): {Colors.ENDC}")
                if query.lower() == 'exit':
                    break
            else:
                break

        except KeyboardInterrupt:
            print(f"\n{Colors.WARNING}Search interrupted{Colors.ENDC}")
            break
        except Exception as e:
            print(f"\n{Colors.RED}Error during search: {e}{Colors.ENDC}")
            break

def view_full_content(result):
    """Display full content of a search result"""
    os.system('cls' if os.name == 'nt' else 'clear')
    print_header(f"VIEWING FULL CONTENT")

    print(f"{Colors.BOLD}{Colors.GREEN}Title:{Colors.ENDC} {result['title']}")
    print(f"{Colors.BOLD}{Colors.BLUE}URL:{Colors.ENDC} {result['url']}")
    if 'date' in result:
        print(f"{Colors.BOLD}{Colors.BLUE}Date:{Colors.ENDC} {result['date']}")
    print(f"{Colors.BOLD}{Colors.BLUE}S3 Key:{Colors.ENDC} {result['s3_key']}")
    print(f"\n{Colors.BOLD}{Colors.GREEN}Description:{Colors.ENDC}")
    print(textwrap.fill(result['description'], width=80))

    # Try to get full content from S3
    try:
        print(f"\n{Colors.CYAN}Retrieving full content from S3...{Colors.ENDC}")
        from s3_storage import retrieve_from_s3
        full_content = retrieve_from_s3(key=result['s3_key'])

        if full_content and 'text_content' in full_content:
            print(f"\n{Colors.BOLD}{Colors.GREEN}Full Content:{Colors.ENDC}\n")

            # Get terminal width
            term_width = os.get_terminal_size().columns if hasattr(os, 'get_terminal_size') else 80

            # Print content with wrapping
            text_content = full_content['text_content']

            # Limit length for very large content
            max_chars = 10000
            if len(text_content) > max_chars:
                text_content = text_content[:max_chars] + f"\n\n{Colors.WARNING}[Content truncated due to length...]{Colors.ENDC}"

            # Print with line wrapping
            for line in text_content.split('\n'):
                print(textwrap.fill(line, width=term_width-5))
        else:
            print(f"\n{Colors.WARNING}Failed to retrieve full text content{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.RED}Error retrieving full content: {e}{Colors.ENDC}")

def main():
    """Command-line interface for search function"""
    parser = argparse.ArgumentParser(description="Search indexed content")
    parser.add_argument("query", nargs="?", help="Search query")
    parser.add_argument("--config", default="crawler_config.json", help="Path to config file")
    parser.add_argument("--output-format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--source", choices=["opensearch", "s3", "file", "all"], default="all",
                       help="Search source (opensearch, s3, file, or all)")
    parser.add_argument("--interactive", "-i", action="store_true", help="Use interactive search interface")
    parser.add_argument("--advanced", "-a", action="store_true", help="Use advanced query syntax (AND, OR, NOT, phrases)")

    args = parser.parse_args()

    if args.interactive or not args.query:
        interactive_search()
        return

    # Choose search function based on source
    if args.source == "opensearch":
        results = search_content(args.query, args.config, advanced=args.advanced)
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
        results = search_content(args.query, args.config, advanced=args.advanced)

    if args.output_format == "json":
        print(json.dumps(results, indent=2))
    else:
        print_header(f"SEARCH RESULTS FOR: {args.query}")

        if not results:
            print(f"{Colors.WARNING}No results found.{Colors.ENDC}")
            return

        for i, result in enumerate(results, 1):
            print_result(i, result)

        print(f"\n{Colors.BOLD}Found {len(results)} results for '{args.query}'{Colors.ENDC}")
        print(f"\n{Colors.CYAN}Tip: Run with --interactive (-i) for an enhanced search experience{Colors.ENDC}")

if __name__ == "__main__":
    main()