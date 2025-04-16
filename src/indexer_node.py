#!/usr/bin/env python3
from mpi4py import MPI
import logging
import time
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def indexer_process(config=None):
    """Indexer node that processes and indexes web content"""
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # This process should only run on indexer nodes
    if config:
        num_crawlers = config['num_crawlers']
        if rank <= num_crawlers or rank > num_crawlers + config['num_indexers']:
            return
    else:
        # Default behavior without config
        if rank != 3:
            return

    logging.info(f"Indexer {rank} starting")

    # Setup output directory for indexed content
    output_dir = config.get('output_dir', 'output') if config else 'output'
    os.makedirs(output_dir, exist_ok=True)
    index_dir = os.path.join(output_dir, f"indexer_{rank}")
    os.makedirs(index_dir, exist_ok=True)

    # Main indexer loop
    while True:
        # Wait for content from master
        status = MPI.Status()
        content = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # Check if we should terminate
        if tag == 10:  # Termination signal
            logging.info(f"Indexer {rank} received termination signal")
            break

        logging.info(f"Indexer {rank} received content to index")

        try:
            # Extract URL from content
            url_line = content.split('\n', 1)[0] if isinstance(content, str) else "Unknown URL"
            url = url_line.replace("URL: ", "")

            # Generate a filename based on URL
            import hashlib
            filename = hashlib.md5(url.encode()).hexdigest() + ".txt"
            filepath = os.path.join(index_dir, filename)

            # Save content to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)

            logging.info(f"Indexer {rank} saved content to {filepath}")

            # In a real implementation, this would also update a search index

            # Send status update to master
            comm.send(f"Indexer {rank} successfully indexed content for {url}", dest=0, tag=99)
            logging.info(f"Indexer {rank} completed indexing")

        except Exception as e:
            # Report any errors to master
            logging.error(f"Indexer {rank} error: {e}")
            comm.send(f"Error indexing: {str(e)}", dest=0, tag=999)

    logging.info(f"Indexer {rank} shutting down")

if __name__ == '__main__':
    indexer_process()