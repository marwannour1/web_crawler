import subprocess
import sys
import os
from crawler_config import CrawlerConfig

base_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, base_dir)


def start_workers():
    # Load config
    config = CrawlerConfig().get_config()
    num_crawlers = config['num_crawlers']
    num_indexers = config['num_indexers']

    print(f"Starting {num_crawlers} crawler workers and {num_indexers} indexer workers...")

    env = os.environ.copy()
    env["PYTHONPATH"] = base_dir + os.pathsep + env.get("PYTHONPATH", "")

    # Start crawler workers
    crawler_cmd = [
        "celery", "-A", "celery_app", "worker",
        "--loglevel=info",
        "--concurrency", str(num_crawlers),
        "-Q", "crawler",
        "-n", f"crawler{os.getpid()}@%h",
        "-P", "solo"
    ]

    # Start indexer workers
    indexer_cmd = [
        "celery", "-A", "celery_app", "worker",
        "--loglevel=info",
        "--concurrency", str(num_indexers),
        "-Q", "indexer",
        "-n", f"indexer{os.getpid()}@%h",
        "-P", "solo"
    ]

    # Start processes
    crawler_proc = subprocess.Popen(crawler_cmd)
    indexer_proc = subprocess.Popen(indexer_cmd)

    print("Workers started. Press Ctrl+C to terminate.")

    try:
        crawler_proc.wait()
        indexer_proc.wait()
    except KeyboardInterrupt:


        crawler_proc.terminate()
        indexer_proc.terminate()
        sys.exit(0)

if __name__ == "__main__":
    start_workers()