broker_url = 'redis://localhost:6379/0'
result_backend = 'redis://localhost:6379/0'
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
enable_utc = True
worker_concurrency = 4  # Number of concurrent worker processes
task_routes = {
    'tasks.crawl': {'queue': 'crawler'},
    'tasks.index': {'queue': 'indexer'},
}