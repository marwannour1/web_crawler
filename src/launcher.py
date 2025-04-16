#!/usr/bin/env python3
from mpi4py import MPI
import os
import sys
from crawler_config import CrawlerConfig
import master_node
import crawler_node
import indexer_node

def main():
    """Main entry point for the distributed web crawler"""

    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Load configuration
    config = CrawlerConfig()
    cfg = config.get_config()

    # Check if we have enough processes
    required_processes = 1 + cfg['num_crawlers'] + cfg['num_indexers']
    if size < required_processes:
        if rank == 0:
            print(f"Error: Not enough MPI processes. Required: {required_processes}, Available: {size}")
            print(f"Please run with: mpiexec -n {required_processes} python src/launcher.py")
        comm.Abort(1)

    # Distribute configuration to all nodes
    cfg = comm.bcast(cfg, root=0)

    # Determine node role based on rank
    if rank == 0:
        # Master node
        master_node.master_process(cfg)
    elif 1 <= rank <= cfg['num_crawlers']:
        # Crawler node
        crawler_node.crawler_process(cfg)
    else:
        # Indexer node
        indexer_node.indexer_process(cfg)

if __name__ == "__main__":
    main()