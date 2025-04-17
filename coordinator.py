import logging
import os
from tasks import crawl
from crawler_config import CrawlerConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def start_crawl(config_file='crawler_config.json'):
    """Start the crawling process by submitting initial tasks"""
    # Load configuration
    config_manager = CrawlerConfig(config_file)
    config = config_manager.get_config()

    # Create output directory
    os.makedirs(config['output_dir'], exist_ok=True)

    logger.info("Starting crawl with configuration:")
    logger.info(f"  Seed URLs: {config['seed_urls']}")
    logger.info(f"  Max Depth: {config['max_depth']}")
    logger.info(f"  Restricted Domains: {config['restricted_domains']}")

    # Submit initial crawl tasks for seed URLs
    task_ids = []
    for url in config['seed_urls']:
        task = crawl.delay(url, 0, config)
        task_ids.append(task.id)
        logger.info(f"Submitted seed URL: {url}, task ID: {task.id}")

    return task_ids

if __name__ == "__main__":
    start_crawl()