#!/usr/bin/env python3
# filepath: crawler_config.py

import os
import json
import logging
from aws_config import get_config_from_s3, store_config_in_s3

class CrawlerConfig:
    """Configuration manager for the distributed web crawler using S3 storage"""

    def __init__(self, config_file='crawler_config.json'):
        self.config_file = config_file
        self.config = {
            'seed_urls': [],
            'restricted_domains': [],
            'max_depth': 3,
            'num_crawlers': 2,
            'num_indexers': 1,
            'output_dir': 'output',
            'request_delay': 1.0,
            'timeout': 10
        }

        # Load existing config if available
        self.load_config()

    def load_config(self):
        """Load configuration from S3 or file if it exists"""
        # First try to load from S3
        s3_config = get_config_from_s3()
        if s3_config:
            self.config.update(s3_config)
            return True

        # Fall back to local file if S3 fails
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    loaded_config = json.load(f)
                    if loaded_config:
                        self.config.update(loaded_config)
                return True
            except Exception as e:
                logging.error(f"Error loading configuration file: {e}")
                return False
        return False

    def save_config(self):
        """Save current configuration to S3 and local file"""
        success = True

        # Save to S3
        s3_success = store_config_in_s3(self.config)
        if not s3_success:
            logging.warning("Could not save config to S3, falling back to local file")
            success = False

        # Always save locally as well
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving configuration file: {e}")
            success = False

        return success

    def get_config(self):
        """Return the current configuration"""
        return self.config