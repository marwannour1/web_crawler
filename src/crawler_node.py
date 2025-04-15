#!/usr/bin/env python3
from mpi4py import MPI
import logging
import time
import sys
import requests
from bs4 import BeautifulSoup
import urllib.parse
import re
import redis
from monitor import send_status_update

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def check_robots_txt(base_url, user_agent="*"):
    """Check robots.txt to see if the crawler is allowed to crawl the URL."""
    try:
        robots_url = urllib.parse.urljoin(base_url, "/robots.txt")
        response = requests.get(robots_url, timeout=5)

        if response.status_code == 200:
            lines = response.text.split('\n')
            current_agent = None
            disallowed_paths = []
            crawl_delay = 0

            for line in lines:
                line = line.strip().lower()
                if not line or line.startswith('#'):
                    continue

                if line.startswith('user-agent:'):
                    agent = line[11:].strip()
                    if agent == '*' or agent == user_agent:
                        current_agent = agent

                elif current_agent and line.startswith('disallow:'):
                    path = line[9:].strip()
                    if path:
                        disallowed_paths.append(path)

                elif current_agent and line.startswith('crawl-delay:'):
                    try:
                        delay = line[12:].strip()
                        crawl_delay = float(delay)
                    except ValueError:
                        crawl_delay = 1

            return {
                'allowed': True,
                'disallowed_paths': disallowed_paths,
                'crawl_delay': crawl_delay
            }

        return {'allowed': True, 'disallowed_paths': [], 'crawl_delay': 1}
    except Exception as e:
        logging.warning(f"Error fetching robots.txt from {base_url}: {e}")
        return {'allowed': True, 'disallowed_paths': [], 'crawl_delay': 1}

def is_allowed_url(url, disallowed_paths):
    """Check if the URL is allowed based on robots.txt rules."""
    parsed_url = urllib.parse.urlparse(url)
    path = parsed_url.path

    for disallowed in disallowed_paths:
        if disallowed == '/' and path == '/':  # Root path
            return False
        if disallowed.endswith('*'):  # Wildcard matching
            if path.startswith(disallowed[:-1]):
                return False
        elif path.startswith(disallowed):
            return False

    return True

def normalize_url(url, base_url):
    """Normalize URL by converting relative URLs to absolute and handling edge cases."""
    try:
        # Handle URLs with or without scheme
        if not url.startswith(('http://', 'https://')):
            url = urllib.parse.urljoin(base_url, url)

        # Parse and rebuild to normalize
        parsed = urllib.parse.urlparse(url)
        # Remove fragments
        url = urllib.parse.urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            parsed.params, parsed.query, ''
        ))

        # Remove trailing slashes for consistency
        if url.endswith('/') and len(url) > 1:
            url = url[:-1]

        return url
    except Exception:
        return None

def is_valid_url(url):
    """Check if the URL is valid and should be crawled."""
    # Skip non-http/https URLs (mailto:, tel:, etc.)
    if not url.startswith(('http://', 'https://')):
        return False

    # Skip common non-HTML file extensions
    if re.search(r'\.(jpg|jpeg|png|gif|pdf|doc|docx|ppt|pptx|zip|rar|exe|css|js)$', url, re.IGNORECASE):
        return False

    return True

def extract_domain(url):
    """Extract the domain from a URL."""
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc

def crawler_process(config=None):
    """Crawler node that fetches web pages"""
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # This process should only run on crawler nodes (ranks 1 and 2 in our example)
    if rank < 1 or rank > 2:
        return

    logging.info(f"Crawler {rank} starting")

    # Track domains we've visited and their robots.txt rules
    domain_rules = {}

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
            # Extract domain and check if we need to get robots.txt rules
            domain = extract_domain(url)
            base_url = f"https://{domain}" if url.startswith("https") else f"http://{domain}"

            # Get robots.txt rules for this domain if we haven't already
            if domain not in domain_rules:
                robots_rules = check_robots_txt(base_url)
                domain_rules[domain] = robots_rules
                logging.info(f"Crawler {rank} - Got robots.txt rules for {domain}: {robots_rules}")
            else:
                robots_rules = domain_rules[domain]

            # Check if we're allowed to crawl this URL
            if not is_allowed_url(url, robots_rules['disallowed_paths']):
                logging.info(f"Crawler {rank} - URL {url} is disallowed by robots.txt")
                result = {
                    'url': url,
                    'new_urls': [],
                    'content': '',
                    'status': 'disallowed'
                }
                comm.send(result, dest=0, tag=1)
                continue

            # Respect crawl delay from robots.txt
            time.sleep(robots_rules['crawl_delay'])

            # Fetch the webpage content
            logging.info(f"Crawler {rank} processing URL: {url}")
            headers = {
                'User-Agent': 'DistributedWebCrawler/1.0 (Academic Project)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
                'Accept-Language': 'en-US,en;q=0.5'
            }
            response = requests.get(url, headers=headers, timeout=10)

            # Check if the response is HTML
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type.lower():
                logging.info(f"Crawler {rank} - URL {url} is not HTML (Content-Type: {content_type})")
                result = {
                    'url': url,
                    'new_urls': [],
                    'content': '',
                    'status': 'not_html'
                }
                comm.send(result, dest=0, tag=1)
                continue

            if response.status_code == 200:
                # Parse HTML and extract links
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract text content for indexing
                # Remove script and style elements that contain JavaScript/CSS
                for script in soup(["script", "style"]):
                    script.extract()

                # Get text content
                text = soup.get_text()

                # Break into lines and remove leading and trailing space on each
                lines = (line.strip() for line in text.splitlines())

                # Break multi-headlines into a line each
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

                # Drop blank lines
                content = '\n'.join(chunk for chunk in chunks if chunk)

                # Extract all links
                raw_links = []
                for link in soup.find_all('a', href=True):
                    raw_links.append(link['href'])

                # Process and filter links
                new_urls = []
                for link in raw_links:
                    normalized_url = normalize_url(link, url)
                    # Only keep valid URLs and optionally limit to same domain
                    if normalized_url and is_valid_url(normalized_url):
                        # Uncomment the following line to limit crawling to the same domain
                        # if extract_domain(normalized_url) == domain:
                        new_urls.append(normalized_url)

                # Limit the number of URLs to avoid overwhelming the system
                new_urls = list(set(new_urls))[:10]  # Deduplicate and limit to 10 new URLs

                # Send results back to master
                result = {
                    'url': url,
                    'new_urls': new_urls,
                    'content': content,
                    'status': 'success'
                }
                comm.send(result, dest=0, tag=1)  # Tag 1 for crawler results
                logging.info(f"Crawler {rank} completed processing URL: {url}, found {len(new_urls)} new URLs")

            else:
                logging.warning(f"Crawler {rank} - Failed to fetch {url}, status code: {response.status_code}")
                result = {
                    'url': url,
                    'new_urls': [],
                    'content': '',
                    'status': f'http_error_{response.status_code}'
                }
                comm.send(result, dest=0, tag=1)

        except requests.exceptions.RequestException as e:
            # Handle network-related exceptions
            logging.error(f"Crawler {rank} request error processing URL {url}: {e}")
            comm.send(f"Error crawling {url}: {str(e)}", dest=0, tag=999)

        except Exception as e:
            # Report any other errors to master
            logging.error(f"Crawler {rank} error processing URL {url}: {e}")
            comm.send(f"Error crawling {url}: {str(e)}", dest=0, tag=999)

    logging.info(f"Crawler {rank} shutting down")

if __name__ == '__main__':
    crawler_process()