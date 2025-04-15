#!/usr/bin/env python3
"""
Main entry point for the distributed web crawler
This script coordinates master, crawler, and indexer nodes
"""
import os
import sys
import json
import argparse
from mpi4py import MPI
from master_node import master_process
from crawler_node import crawler_process
from indexer_node import indexer_process

def main():
    """Main entry point"""
    # Get the MPI communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Only rank 0 (master) parses arguments to avoid conflicts
    config = {}
    if rank == 0:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description="Distributed Web Crawler")
        parser.add_argument("--config", help="Path to JSON configuration file")
        parser.add_argument("--seed-urls", nargs="+", help="Seed URLs to start crawling")
        parser.add_argument("--restricted-urls", nargs="+", help="Restricted URLs to avoid crawling")
        parser.add_argument("--max-depth", type=int, default=3, help="Maximum crawl depth")
        parser.add_argument("--max-urls-per-domain", type=int, default=50,
                            help="Maximum URLs to crawl per domain")
        args = parser.parse_args()

        # If config file is provided, load it
        if args.config and os.path.exists(args.config):
            with open(args.config, 'r') as f:
                config = json.load(f)
        else:
            # Create a configuration from command line arguments
            config = {
                'seed_urls': args.seed_urls or [],
                'restricted_urls': args.restricted_urls or [],
                'max_depth': args.max_depth,
                'max_urls_per_domain': args.max_urls_per_domain or 50
            }

    # Broadcast the configuration from rank 0 to all processes
    config = comm.bcast(config, root=0)

    # Start the appropriate process based on rank
    if rank == 0:
        # Master node
        master_process(config)
    elif rank <= size - 2:  # All processes except master and last one
        # Crawler nodes
        crawler_process(config)
    else:
        # Indexer node (last process)
        indexer_process(config)

if __name__ == "__main__":
    main()