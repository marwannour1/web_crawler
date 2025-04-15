#!/usr/bin/env python3
"""
Indexer node for the distributed web crawler
This node indexes the content crawled by crawler nodes
"""
import os
import sys
import logging
import time
from mpi4py import MPI
import redis
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import whoosh.index as index
from monitor import send_status_update

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def indexer_process(config=None):
    """Indexer node process implementation"""
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    # This process should only run on the indexer node (last rank)
    if rank != size - 1:
        return

    logging.info(f"Indexer {rank} starting")

    # Initialize Redis for monitoring
    try:
        redis_client = redis.Redis(host='172.28.79.201', port=6379, db=0)
        send_status_update(redis_client, 'indexer', rank, 'Starting')
    except Exception as e:
        logging.error(f"Error connecting to Redis: {e}")
        redis_client = None

    # Set up the schema for Whoosh
    schema = Schema(
        url=ID(stored=True, unique=True),
        content=TEXT(stored=True)
    )

    # Create or open an index
    index_dir = f"index_node_{rank}"
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
        ix = create_in(index_dir, schema)
        logging.info(f"Created new index in {index_dir}")
    else:
        if index.exists_in(index_dir):
            ix = open_dir(index_dir)
            logging.info(f"Opened existing index in {index_dir}")
        else:
            ix = create_in(index_dir, schema)
            logging.info(f"Created new index in {index_dir}")

    # Statistics
    processed_count = 0
    start_time = time.time()

    # Main indexing loop
    writer = ix.writer()
    batch_size = 0
    commit_threshold = 10  # Commit after this many documents

    while True:
        # Check for messages
        status = MPI.Status()
        if comm.Iprobe(source=0, tag=MPI.ANY_TAG, status=status):
            tag = status.Get_tag()

            if tag == 10:  # Termination signal
                # Receive the termination message to clear the buffer
                comm.recv(source=0, tag=tag)

                # Commit any remaining changes
                if batch_size > 0:
                    writer.commit()
                    logging.info(f"Committed final batch of {batch_size} documents")

                logging.info(f"Indexer received termination signal. Indexed {processed_count} pages total")

                if redis_client:
                    elapsed_time = time.time() - start_time
                    send_status_update(redis_client, 'indexer', rank, 'Completed',
                                      processed=processed_count,
                                      elapsed_time=elapsed_time)

                break

            elif tag == 2:  # Document to index
                data = comm.recv(source=0, tag=tag)
                url = data['url']
                content = data['content']

                try:
                    # Add the document to the index
                    writer.update_document(url=url, content=content)
                    batch_size += 1
                    processed_count += 1

                    # Commit periodically
                    if batch_size >= commit_threshold:
                        writer.commit()
                        writer = ix.writer()
                        logging.info(f"Committed batch of {batch_size} documents. Total: {processed_count}")
                        batch_size = 0

                    # Report indexing
                    if redis_client and processed_count % 5 == 0:  # Report every 5 documents
                        send_status_update(redis_client, 'indexer', rank, 'Indexing',
                                         processed=processed_count,
                                         current_url=url)

                except Exception as e:
                    logging.error(f"Error indexing {url}: {e}")
                    if redis_client:
                        send_status_update(redis_client, 'indexer', rank, 'Error indexing',
                                         error=True, error_msg=str(e))

        else:
            # No messages, sleep briefly
            time.sleep(0.1)

    logging.info(f"Indexer {rank} shutting down")

if __name__ == '__main__':
    indexer_process()