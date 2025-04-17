from celery import Celery
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


app = Celery('web_crawler')
app.config_from_object('celeryconfig')

# This ensures tasks are loaded when Celery starts
try:
    import tasks
except ImportError:
    print("Error importing tasks. Make sure the tasks module is available.")
    sys.exit(1)

if __name__ == '__main__':
    app.start()