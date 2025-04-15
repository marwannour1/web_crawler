#!/usr/bin/env python3
"""
Monitor for the distributed web crawler
This script provides monitoring capabilities for tracking node status
"""
import os
import sys
import json
import time
import redis
import logging
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class CrawlerMonitor:
    """Monitors the status of distributed crawler nodes"""

    def __init__(self):
        """Initialize the monitor"""
        # Connect to Redis for node communication
        try:
            self.redis = redis.Redis(host='172.28.79.201', port=6379, db=0)
            self.pubsub = self.redis.pubsub()
            self.pubsub.subscribe('crawler_status')
            logging.info("Connected to Redis for monitoring")
        except Exception as e:
            logging.error(f"Error connecting to Redis: {e}")
            self.redis = None

        # Node status tracking
        self.nodes = {
            'master': {'status': 'Unknown', 'last_update': None, 'processed': 0},
            'crawlers': {},
            'indexer': {'status': 'Unknown', 'last_update': None, 'processed': 0}
        }

        # Statistics
        self.stats = {
            'urls_queued': 0,
            'urls_crawled': 0,
            'urls_indexed': 0,
            'start_time': None,
            'errors': 0
        }

    def start_listener(self):
        """Start listening for status updates from nodes"""
        if not self.redis:
            logging.error("Cannot start listener: Redis not connected")
            return

        def listen_for_updates():
            logging.info("Starting monitor listener thread")
            for message in self.pubsub.listen():
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        self.process_status_update(data)
                    except json.JSONDecodeError:
                        logging.error(f"Invalid JSON in status update: {message['data']}")
                    except Exception as e:
                        logging.error(f"Error processing status update: {e}")

        # Start the listener in a separate thread
        threading.Thread(target=listen_for_updates, daemon=True).start()

    def process_status_update(self, data):
        """Process a status update from a node"""
        node_type = data.get('node_type')
        node_id = data.get('node_id')
        status = data.get('status')
        timestamp = data.get('timestamp', time.time())

        if node_type == 'master':
            self.nodes['master']['status'] = status
            self.nodes['master']['last_update'] = timestamp
            if 'queue_size' in data:
                self.stats['urls_queued'] = data['queue_size']

        elif node_type == 'crawler':
            if node_id not in self.nodes['crawlers']:
                self.nodes['crawlers'][node_id] = {
                    'status': status,
                    'last_update': timestamp,
                    'processed': 0,
                    'current_url': None
                }
            else:
                self.nodes['crawlers'][node_id]['status'] = status
                self.nodes['crawlers'][node_id]['last_update'] = timestamp

            if 'current_url' in data:
                self.nodes['crawlers'][node_id]['current_url'] = data['current_url']

            if 'processed' in data:
                self.nodes['crawlers'][node_id]['processed'] = data['processed']
                # Update total crawled count
                self.stats['urls_crawled'] = sum(c.get('processed', 0) for c in self.nodes['crawlers'].values())

        elif node_type == 'indexer':
            self.nodes['indexer']['status'] = status
            self.nodes['indexer']['last_update'] = timestamp
            if 'processed' in data:
                self.nodes['indexer']['processed'] = data['processed']
                self.stats['urls_indexed'] = data['processed']

        if 'error' in data and data['error']:
            self.stats['errors'] += 1

        # If this is the first message and we haven't set a start time
        if self.stats['start_time'] is None:
            self.stats['start_time'] = timestamp

def send_status_update(redis_client, node_type, node_id, status, **kwargs):
    """
    Send a status update to the monitor

    Args:
        redis_client: Redis client instance
        node_type: Type of node ('master', 'crawler', 'indexer')
        node_id: ID of the node (typically the rank)
        status: Current status message
        **kwargs: Additional data to include in the update
    """
    if not redis_client:
        return

    data = {
        'node_type': node_type,
        'node_id': node_id,
        'status': status,
        'timestamp': time.time(),
        **kwargs
    }

    try:
        redis_client.publish('crawler_status', json.dumps(data))
    except Exception as e:
        logging.error(f"Error sending status update: {e}")

class MonitorGUI:
    """GUI for the crawler monitor"""

    def __init__(self, root):
        """Initialize the GUI"""
        self.root = root
        self.root.title("Distributed Web Crawler Monitor")
        self.root.geometry("900x700")

        # Create the monitor
        self.monitor = CrawlerMonitor()

        # Set up the main frame with padding
        main_frame = ttk.Frame(root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Title label
        title_label = ttk.Label(
            main_frame,
            text="Distributed Web Crawler Monitor",
            font=("Arial", 16)
        )
        title_label.pack(pady=10)

        # Create notebook with tabs
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=10)

        # Dashboard tab
        dashboard_tab = ttk.Frame(notebook)
        notebook.add(dashboard_tab, text="Dashboard")
        self.setup_dashboard(dashboard_tab)

        # Nodes tab
        nodes_tab = ttk.Frame(notebook)
        notebook.add(nodes_tab, text="Node Details")
        self.setup_nodes_tab(nodes_tab)

        # Logs tab
        logs_tab = ttk.Frame(notebook)
        notebook.add(logs_tab, text="Logs")
        self.setup_logs_tab(logs_tab)

        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Monitor ready")
        status_bar = ttk.Label(
            root,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        # Start monitoring
        self.monitor.start_listener()
        self.update_gui()

    def setup_dashboard(self, parent):
        """Set up the dashboard tab"""
        # Status indicators section
        status_frame = ttk.LabelFrame(parent, text="Node Status")
        status_frame.pack(fill=tk.X, padx=10, pady=5)

        # Master node status
        ttk.Label(status_frame, text="Master:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.master_status = ttk.Label(status_frame, text="Unknown", foreground="gray")
        self.master_status.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)

        # Crawler nodes status
        ttk.Label(status_frame, text="Crawlers:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.crawler_status = ttk.Label(status_frame, text="Unknown", foreground="gray")
        self.crawler_status.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)

        # Indexer node status
        ttk.Label(status_frame, text="Indexer:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
        self.indexer_status = ttk.Label(status_frame, text="Unknown", foreground="gray")
        self.indexer_status.grid(row=2, column=1, padx=5, pady=5, sticky=tk.W)

        # Statistics section
        stats_frame = ttk.LabelFrame(parent, text="Crawl Statistics")
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # URLs stats
        ttk.Label(stats_frame, text="URLs Queued:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.urls_queued = ttk.Label(stats_frame, text="0")
        self.urls_queued.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(stats_frame, text="URLs Crawled:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.urls_crawled = ttk.Label(stats_frame, text="0")
        self.urls_crawled.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(stats_frame, text="URLs Indexed:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
        self.urls_indexed = ttk.Label(stats_frame, text="0")
        self.urls_indexed.grid(row=2, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(stats_frame, text="Errors:").grid(row=3, column=0, padx=5, pady=5, sticky=tk.W)
        self.errors = ttk.Label(stats_frame, text="0")
        self.errors.grid(row=3, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(stats_frame, text="Running Time:").grid(row=4, column=0, padx=5, pady=5, sticky=tk.W)
        self.running_time = ttk.Label(stats_frame, text="00:00:00")
        self.running_time.grid(row=4, column=1, padx=5, pady=5, sticky=tk.W)

        # Progress bar
        ttk.Label(stats_frame, text="Progress:").grid(row=5, column=0, padx=5, pady=5, sticky=tk.W)
        self.progress_bar = ttk.Progressbar(stats_frame, orient=tk.HORIZONTAL, length=200, mode='indeterminate')
        self.progress_bar.grid(row=5, column=1, padx=5, pady=5, sticky=tk.W)
        self.progress_bar.start(10)

    def setup_nodes_tab(self, parent):
        """Set up the nodes tab with detailed node information"""
        # Master node section
        master_frame = ttk.LabelFrame(parent, text="Master Node")
        master_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(master_frame, text="Status:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.master_detail_status = ttk.Label(master_frame, text="Unknown")
        self.master_detail_status.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(master_frame, text="Queue Size:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.master_queue_size = ttk.Label(master_frame, text="0")
        self.master_queue_size.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(master_frame, text="Last Update:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
        self.master_last_update = ttk.Label(master_frame, text="Never")
        self.master_last_update.grid(row=2, column=1, padx=5, pady=5, sticky=tk.W)

        # Crawler nodes section
        crawler_frame = ttk.LabelFrame(parent, text="Crawler Nodes")
        crawler_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Create a treeview for crawlers
        columns = ('id', 'status', 'processed', 'current_url', 'last_update')
        self.crawler_tree = ttk.Treeview(crawler_frame, columns=columns, show='headings')

        # Define column headings
        self.crawler_tree.heading('id', text='Node ID')
        self.crawler_tree.heading('status', text='Status')
        self.crawler_tree.heading('processed', text='URLs Processed')
        self.crawler_tree.heading('current_url', text='Current URL')
        self.crawler_tree.heading('last_update', text='Last Update')

        # Set column widths
        self.crawler_tree.column('id', width=50)
        self.crawler_tree.column('status', width=100)
        self.crawler_tree.column('processed', width=100)
        self.crawler_tree.column('current_url', width=300)
        self.crawler_tree.column('last_update', width=150)

        # Add a scrollbar
        scrollbar = ttk.Scrollbar(crawler_frame, orient=tk.VERTICAL, command=self.crawler_tree.yview)
        self.crawler_tree.configure(yscroll=scrollbar.set)

        # Pack the treeview and scrollbar
        self.crawler_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Indexer node section
        indexer_frame = ttk.LabelFrame(parent, text="Indexer Node")
        indexer_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(indexer_frame, text="Status:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.indexer_detail_status = ttk.Label(indexer_frame, text="Unknown")
        self.indexer_detail_status.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(indexer_frame, text="URLs Indexed:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.indexer_processed = ttk.Label(indexer_frame, text="0")
        self.indexer_processed.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(indexer_frame, text="Last Update:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
        self.indexer_last_update = ttk.Label(indexer_frame, text="Never")
        self.indexer_last_update.grid(row=2, column=1, padx=5, pady=5, sticky=tk.W)

    def setup_logs_tab(self, parent):
        """Set up the logs tab"""
        # Create log text area with scrollbar
        self.log_text = scrolledtext.ScrolledText(
            parent,
            wrap=tk.WORD,
            background='black',
            foreground='white',
            font=("Consolas", 10)
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text.config(state=tk.DISABLED)  # Make read-only

    def update_gui(self):
        """Update the GUI with the latest monitoring data"""
        try:
            # Update dashboard
            # Master status
            master_status = self.monitor.nodes['master']['status']
            self.master_status.config(
                text=master_status,
                foreground=self.get_status_color(master_status)
            )

            # Crawler status - summary of all crawlers
            crawler_count = len(self.monitor.nodes['crawlers'])
            if crawler_count > 0:
                active_count = sum(1 for c in self.monitor.nodes['crawlers'].values()
                                if c['status'] not in ['Unknown', 'Idle', 'Error'])
                self.crawler_status.config(
                    text=f"{active_count}/{crawler_count} Active",
                    foreground="green" if active_count > 0 else "orange"
                )
            else:
                self.crawler_status.config(text="Unknown", foreground="gray")

            # Indexer status
            indexer_status = self.monitor.nodes['indexer']['status']
            self.indexer_status.config(
                text=indexer_status,
                foreground=self.get_status_color(indexer_status)
            )

            # Statistics
            self.urls_queued.config(text=str(self.monitor.stats['urls_queued']))
            self.urls_crawled.config(text=str(self.monitor.stats['urls_crawled']))
            self.urls_indexed.config(text=str(self.monitor.stats['urls_indexed']))
            self.errors.config(text=str(self.monitor.stats['errors']))

            # Running time
            if self.monitor.stats['start_time']:
                elapsed = time.time() - self.monitor.stats['start_time']
                hours, remainder = divmod(int(elapsed), 3600)
                minutes, seconds = divmod(remainder, 60)
                self.running_time.config(text=f"{hours:02d}:{minutes:02d}:{seconds:02d}")

            # Update nodes tab
            # Master node details
            master = self.monitor.nodes['master']
            self.master_detail_status.config(
                text=master['status'],
                foreground=self.get_status_color(master['status'])
            )
            self.master_queue_size.config(text=str(self.monitor.stats['urls_queued']))
            if master['last_update']:
                self.master_last_update.config(
                    text=datetime.fromtimestamp(master['last_update']).strftime('%H:%M:%S')
                )

            # Crawler nodes - clear and rebuild treeview
            for item in self.crawler_tree.get_children():
                self.crawler_tree.delete(item)

            for node_id, crawler in self.monitor.nodes['crawlers'].items():
                last_update = "Never"
                if crawler['last_update']:
                    last_update = datetime.fromtimestamp(crawler['last_update']).strftime('%H:%M:%S')

                current_url = crawler.get('current_url', "None")
                if current_url and len(current_url) > 40:
                    current_url = current_url[:37] + "..."

                self.crawler_tree.insert(
                    '', 'end', values=(
                        node_id,
                        crawler['status'],
                        crawler.get('processed', 0),
                        current_url,
                        last_update
                    )
                )

            # Indexer node details
            indexer = self.monitor.nodes['indexer']
            self.indexer_detail_status.config(
                text=indexer['status'],
                foreground=self.get_status_color(indexer['status'])
            )
            self.indexer_processed.config(text=str(indexer.get('processed', 0)))
            if indexer['last_update']:
                self.indexer_last_update.config(
                    text=datetime.fromtimestamp(indexer['last_update']).strftime('%H:%M:%S')
                )

            # Add log messages for significant events
            for node_id, crawler in self.monitor.nodes['crawlers'].items():
                if crawler.get('current_url') and crawler.get('last_update') and time.time() - crawler['last_update'] < 2:
                    self.add_log(f"Crawler {node_id} processing: {crawler.get('current_url')}")

            # Schedule the next update
            self.root.after(1000, self.update_gui)

        except Exception as e:
            logging.error(f"Error updating monitor GUI: {e}")
            self.status_var.set(f"Error: {str(e)}")
            # Still try to schedule the next update
            self.root.after(1000, self.update_gui)

    def add_log(self, message):
        """Add a log message to the log tab"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = f"[{timestamp}] {message}\n"

        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)  # Auto-scroll
        self.log_text.config(state=tk.DISABLED)

    def get_status_color(self, status):
        """Get color for a status message"""
        status_lower = status.lower()
        if "idle" in status_lower or "waiting" in status_lower:
            return "orange"
        elif "error" in status_lower or "fail" in status_lower:
            return "red"
        elif "unknown" in status_lower:
            return "gray"
        else:
            return "green"  # Active status


def main():
    """Main entry point for the monitor application"""
    root = tk.Tk()
    app = MonitorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()