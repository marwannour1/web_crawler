#!/usr/bin/env python3
import os
import json
import logging

class CrawlerConfig:
    """Configuration manager for the distributed web crawler"""

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
        """Load configuration from file if it exists"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    loaded_config = json.load(f)
                    if loaded_config:
                        self.config.update(loaded_config)
                return True
            except Exception as e:
                logging.error(f"Error loading configuration: {e}")
                return False
        return False

    def save_config(self):
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            return True
        except Exception as e:
            logging.error(f"Error saving configuration: {e}")
            return False

    def get_config(self):
        """Return the current configuration"""
        return self.config