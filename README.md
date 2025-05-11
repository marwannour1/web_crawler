Installation & Setup 
 Prerequisites 
    •	Python 3.7+ 
    •	AWS Account with appropriate permissions 
    •	SSH access to EC2 instances 
    •	Required Python packages (see requirements.txt) 
Environment Setup 
    Set the following environment variables: 
    export AWS_ACCESS_KEY_ID="your_access_key" 
    export AWS_SECRET_ACCESS_KEY="your_secret_key" 
    export AWS_REGION="your_region" 
    export MASTER_IP="master_node_ip" 
    export CRAWLER_IP="crawler_node_ip" 
    export INDEXER_IP="indexer_node_ip" 
    export AWS_SSH_KEY_PATH="path/to/ssh_key.pem" 
    export OPENSEARCH_ENDPOINT="your_opensearch_endpoint" 
    export OPENSEARCH_USER="elastic" 
    export OPENSEARCH_PASS="your_password" 
Install dependencies: 
    pip install -r requirements.txt 
Running the System 
    Starting All Components 
        Start the entire system using the client: 
        python crawler_client.py  
        This will: 
        1.	Check node status via SSH 
        2.	Start any nodes that aren't running 
        3.	Verify components start successfully 
        4.	Display the dashboard interface 
        2.2.2 Using the Dashboard Interface 
        The dashboard provides a unified management interface: 
        SYSTEM COMPONENTS STATUS 
        ============================================================ 
        Component    Status     Message                               
        -----------  ---------  --------------------------------------- 
        Master       ✓ RUNNING   
        Crawler      ✓ RUNNING   
        Indexer      ✓ RUNNING   
        CRAWLING STATISTICS 
        ============================================================ 
        Crawled Pages: 127 
        Crawler Queue: 243 tasks 
        Indexer Queue: 12 tasks 
        COMMANDS 
        ============================================================ 
        1. Start new crawl 
        2. Search crawled content 
        3. Purge crawled data 
        4. View/modify configuration 
        r. Refresh dashboard 
        q. Quit 
        Commands: 
        •	1: Start a new crawl with current seed URLs 
        •	2: Search indexed content with highlighting and pagination 
        •	3: Purge all crawled data from S3, DynamoDB, and OpenSearch 
        •	4: Modify system configuration 
        •	r: Refresh the dashboard 
        •	q: Quit the client 
Starting a Crawl 
    1.	Select option 1 from the dashboard 
    2.	Choose to use existing seed URLs 
    3.	Confirm crawl parameters (depth, crawlers, etc.) 
    4.	If nodes aren't running, you'll be prompted to start them 
    5.	Monitor the crawl progress in real-time: 
        [13:21:15] Crawled pages: 15 (+15) | Crawler queue: 76 | Indexer queue: 0 | Stable for: 12s 
        Crawling is complete when: 
        •	Both queues are empty (0 tasks) 
        •	Content count has been stable for 30+ seconds 
        •	A completion message is displayed 
        2.4 Searching Crawled Content 
        1.	Select option 2 from the dashboard or run: 
        python crawler_client.py --search 
        2.	Enter your search query when prompted 
        3.	Navigate search results: 
        o	n: Next page 
        o	p: Previous page 
        o	v [number]: View full content of a result 
        o	q: Enter a new search query 
        o	exit: Return to dashboard 
    4.	Search uses OpenSearch by default, falling back to S3 direct search if necessary 
 
Modifying Configuration 
    1.	Select option 4 from the dashboard 
    2.	Current configuration will be displayed: 
        Current Configuration: 
        1. Seed URLs: example.com, example.org 
        2. Max Depth: 3 
        3. Number of Crawlers: 6 
        4. Number of Indexers: 2 
        5. Request Delay: 1.0 seconds 
        6. Request Timeout: 10 seconds 
        7. OpenSearch Index Name: webcrawler 
        8. Restricted Domains: example.com, example.org 
    3.	Enter the number to modify that setting 
    4.	Enter the new value 
    5.	Select s to save or r to return without saving 
Purging Data 
    1.	Select option 3 from the dashboard 
    2.	Type PURGE to confirm deletion 
    3.	The system will purge: 
        o	S3 content in the output directory 
        o	DynamoDB table (recreating the structure) 
        o	OpenSearch index 

