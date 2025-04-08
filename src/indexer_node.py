#!/usr/bin/env python3
from mpi4py import MPI
import logging
import time
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def indexer_process():
    """Indexer node that processes and indexes web content"""
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # This process should only run on indexer nodes (rank 3 in our example)
    if rank != 3:
        return

    logging.info(f"Indexer {rank} starting")

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
            # Simulate indexing process
            logging.info(f"Indexer {rank} processing content")
            time.sleep(1.5)  # Simulate indexing time

            # In a real implementation:
            # 1. Parse the content
            # 2. Extract keywords
            # 3. Update the search index (e.g., using Whoosh, Elasticsearch)

            # Send status update to master
            comm.send(f"Indexer {rank} successfully indexed content", dest=0, tag=99)
            logging.info(f"Indexer {rank} completed indexing")

        except Exception as e:
            # Report any errors to master
            logging.error(f"Indexer {rank} error: {e}")
            comm.send(f"Error indexing: {str(e)}", dest=0, tag=999)

    logging.info(f"Indexer {rank} shutting down")

if __name__ == '__main__':
    indexer_process()