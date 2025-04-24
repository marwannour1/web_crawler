#!/usr/bin/env python3
"""
Worker launcher script that ensures tasks are registered properly.
"""
import os
import sys

# Import tasks BEFORE importing app
import tasks
from tasks import crawl, index
from celery_app import app

# Print registered tasks for debugging
print("Registered tasks:")
for task_name in app.tasks.keys():
    if not task_name.startswith('celery.'):
        print(f"  - {task_name}")

# Start the worker process
if __name__ == '__main__':
    argv = sys.argv[1:]  # Skip the script name
    argv = ['worker'] + argv
    app.worker_main(argv)