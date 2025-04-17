#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import subprocess
import threading
import queue
import sys
import time
from crawler_config import CrawlerConfig
from coordinator import start_crawl
from celery.result import AsyncResult
from celery_app import app
import subprocess


class StdoutRedirector:
    def __init__(self, text_widget):
        self.text_widget = text_widget
        self.buffer = queue.Queue()
        self.running = True
        self.update_thread = threading.Thread(target=self.update_text_widget)
        self.update_thread.daemon = True
        self.update_thread.start()

    def write(self, string):
        self.buffer.put(string)

    def flush(self):
        pass

    def update_text_widget(self):
        while self.running:
            try:
                while not self.buffer.empty():
                    text = self.buffer.get_nowait()
                    self.text_widget.config(state=tk.NORMAL)
                    self.text_widget.insert(tk.END, text)
                    self.text_widget.see(tk.END)
                    self.text_widget.config(state=tk.DISABLED)
            except queue.Empty:
                pass
            time.sleep(0.1)


class CrawlerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Distributed Web Crawler")
        self.root.geometry("900x650")
        self.root.minsize(800, 600)
        self.config_file = "crawler_config.json"
        self.process = None
        self.task_ids = []
        self.monitoring_paused = False  # Track if monitoring is paused
        self.pause_event = threading.Event()  # Event for pausing/resuming


        # Default configuration
        self.config_manager = CrawlerConfig()
        self.config = self.config_manager.get_config()

        # Create main frames
        self.create_widgets()

    def create_widgets(self):
        # Create main frames
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Configuration tab
        self.config_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.config_tab, text="Configuration")

        # Monitor tab
        self.monitor_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.monitor_tab, text="Monitor")

        # Setup configuration tab
        self.setup_config_tab()

        # Setup monitor tab
        self.setup_monitor_tab()

        # Control buttons at bottom
        self.control_frame = ttk.Frame(self.main_frame)
        self.control_frame.pack(fill=tk.X, pady=10)

        self.start_button = ttk.Button(self.control_frame, text="Start Crawler",
                                       command=self.start_crawler)
        self.start_button.pack(side=tk.LEFT, padx=5)

        self.stop_button = ttk.Button(self.control_frame, text="Stop Crawler",
                                      command=self.stop_crawler, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        self.save_button = ttk.Button(self.control_frame, text="Save Configuration",
                                     command=self.save_config)
        self.save_button.pack(side=tk.RIGHT, padx=5)


    def setup_monitor_tab(self):
        # Create console output
        console_frame = ttk.LabelFrame(self.monitor_tab, text="Crawler Output")
        console_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.console_text = scrolledtext.ScrolledText(console_frame, wrap=tk.WORD, state=tk.DISABLED)
        self.console_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Control buttons
        controls_frame = ttk.Frame(self.monitor_tab)
        controls_frame.pack(fill=tk.X, pady=5)

        # Add pause/resume button
        self.pause_button = ttk.Button(controls_frame, text="Pause Monitor",
                                    command=self.toggle_monitor_pause)
        self.pause_button.pack(side=tk.LEFT, padx=5)

        ttk.Button(controls_frame, text="Clear Console",
                command=self.clear_console).pack(side=tk.RIGHT, padx=5)

    def setup_config_tab(self):
        # Left and right frames
        left_frame = ttk.Frame(self.config_tab)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        right_frame = ttk.Frame(self.config_tab)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Seed URLs section
        urls_frame = ttk.LabelFrame(left_frame, text="Seed URLs")
        urls_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.urls_text = scrolledtext.ScrolledText(urls_frame, height=8)
        self.urls_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Set existing URLs if any
        if self.config['seed_urls']:
            self.urls_text.insert(tk.END, '\n'.join(self.config['seed_urls']))

        # Restricted domains section
        domains_frame = ttk.LabelFrame(left_frame, text="Restricted Domains (Optional)")
        domains_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.domains_text = scrolledtext.ScrolledText(domains_frame, height=8)
        self.domains_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Set existing domains if any
        if self.config['restricted_domains']:
            self.domains_text.insert(tk.END, '\n'.join(self.config['restricted_domains']))

        # Crawler parameters section
        params_frame = ttk.LabelFrame(right_frame, text="Crawler Parameters")
        params_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Max depth
        depth_frame = ttk.Frame(params_frame)
        depth_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(depth_frame, text="Max Depth:").pack(side=tk.LEFT)
        self.depth_var = tk.IntVar(value=self.config['max_depth'])
        ttk.Spinbox(depth_frame, from_=1, to=10, textvariable=self.depth_var, width=5).pack(side=tk.LEFT, padx=5)

        # Number of crawlers
        crawlers_frame = ttk.Frame(params_frame)
        crawlers_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(crawlers_frame, text="Number of Crawlers:").pack(side=tk.LEFT)
        self.crawlers_var = tk.IntVar(value=self.config['num_crawlers'])
        ttk.Spinbox(crawlers_frame, from_=1, to=20, textvariable=self.crawlers_var, width=5).pack(side=tk.LEFT, padx=5)

        # Number of indexers
        indexers_frame = ttk.Frame(params_frame)
        indexers_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(indexers_frame, text="Number of Indexers:").pack(side=tk.LEFT)
        self.indexers_var = tk.IntVar(value=self.config['num_indexers'])
        ttk.Spinbox(indexers_frame, from_=1, to=10, textvariable=self.indexers_var, width=5).pack(side=tk.LEFT, padx=5)

        # Request delay
        delay_frame = ttk.Frame(params_frame)
        delay_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(delay_frame, text="Request Delay (seconds):").pack(side=tk.LEFT)
        self.delay_var = tk.DoubleVar(value=self.config['request_delay'])
        ttk.Spinbox(delay_frame, from_=0.1, to=10.0, increment=0.1, textvariable=self.delay_var, width=5).pack(side=tk.LEFT, padx=5)

        # Timeout
        timeout_frame = ttk.Frame(params_frame)
        timeout_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(timeout_frame, text="Timeout (seconds):").pack(side=tk.LEFT)
        self.timeout_var = tk.IntVar(value=self.config['timeout'])
        ttk.Spinbox(timeout_frame, from_=1, to=60, textvariable=self.timeout_var, width=5).pack(side=tk.LEFT, padx=5)

        # Output directory
        output_frame = ttk.Frame(params_frame)
        output_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(output_frame, text="Output Directory:").pack(side=tk.LEFT)
        self.output_var = tk.StringVar(value=self.config['output_dir'])
        ttk.Entry(output_frame, textvariable=self.output_var).pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        ttk.Button(output_frame, text="Browse", command=self.browse_output_dir).pack(side=tk.RIGHT)

    def browse_output_dir(self):
        directory = filedialog.askdirectory(initialdir=".", title="Select Output Directory")
        if directory:
            self.output_var.set(directory)

    def clear_console(self):
        self.console_text.config(state=tk.NORMAL)
        self.console_text.delete(1.0, tk.END)
        self.console_text.config(state=tk.DISABLED)

    def toggle_monitor_pause(self):
        """Toggle pause/resume monitoring"""
        self.monitoring_paused = not self.monitoring_paused

        if self.monitoring_paused:
            self.pause_button.config(text="Resume Monitor")
            self.pause_event.clear()  # Block the monitoring thread
            print("\n>>> MONITORING PAUSED - Current state frozen <<<\n")
        else:
            self.pause_button.config(text="Pause Monitor")
            self.pause_event.set()    # Unblock the monitoring thread
            print("\n>>> MONITORING RESUMED <<<\n")

    def save_config(self):
        # Gather configuration from UI
        self.config['max_depth'] = self.depth_var.get()
        self.config['num_crawlers'] = self.crawlers_var.get()
        self.config['num_indexers'] = self.indexers_var.get()
        self.config['request_delay'] = self.delay_var.get()
        self.config['timeout'] = self.timeout_var.get()
        self.config['output_dir'] = self.output_var.get()

        # Process URLs and domains
        urls = self.urls_text.get(1.0, tk.END).strip()
        self.config['seed_urls'] = [url.strip() for url in urls.split('\n') if url.strip()]

        domains = self.domains_text.get(1.0, tk.END).strip()
        self.config['restricted_domains'] = [domain.strip() for domain in domains.split('\n') if domain.strip()]

        # Update config manager and save
        self.config_manager.config = self.config

        # Save to file
        if self.config_manager.save_config():
            messagebox.showinfo("Success", "Configuration saved successfully.")
        else:
            messagebox.showerror("Error", "Failed to save configuration.")

    def start_crawler(self):
        # Save config first
        self.save_config()

        # Redirect stdout to the console text widget
        self.redirector = StdoutRedirector(self.console_text)
        sys.stdout = self.redirector

        # Make sure workers are running
        if not self.ensure_workers_running():
            print("Failed to start worker processes. Cannot continue.")
            self.reset_buttons()
            return

        # Start the crawler in a separate thread to avoid blocking the GUI
        self.crawler_thread = threading.Thread(target=self.run_crawler_process)
        self.crawler_thread.daemon = True
        self.crawler_thread.start()

        # Update button states
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)

        # Switch to monitor tab
        self.notebook.select(1)

    def run_crawler_process(self):
        try:
            # Task limits
            max_tasks = 100  # Maximum tasks to allow

            print("Starting distributed web crawler with Celery...")
            print(f"Using {self.config['num_crawlers']} crawler workers and {self.config['num_indexers']} indexer workers")

            # Start the crawling process
            self.task_ids = start_crawl()
            print(f"Submitted {len(self.task_ids)} initial crawling tasks")

            # Get initial tasks
            initial_tasks = [AsyncResult(task_id, app=app) for task_id in self.task_ids]

            # Monitor task progress with overall timeout
            start_time = time.time()
            max_runtime = 120  # 2 minutes max before giving up
            last_status_time = time.time()
            timeout_counter = 0

            # Initialize the pause event
            self.pause_event.set()

            # Monitoring loop
            while True:
                # Check for pause
                self.pause_event.wait()

                # Get active task count from Celery
                active_tasks = app.control.inspect().active()
                reserved_tasks = app.control.inspect().reserved()

                # Count active tasks and check limits
                active_count = 0
                task_count = 0

                if active_tasks:
                    task_count = sum(len(tasks) for tasks in active_tasks.values())
                    active_count += task_count

                    if task_count > max_tasks:
                        print(f"Task count ({task_count}) exceeded maximum ({max_tasks}). Stopping crawler.")
                        break

                if reserved_tasks:
                    active_count += sum(len(tasks) for tasks in reserved_tasks.values())

                # Check timeouts
                if time.time() - start_time > max_runtime:
                    print("Maximum runtime exceeded. Stopping monitoring.")
                    break

                # Check if all initial tasks are complete
                all_initial_done = all(task.ready() for task in initial_tasks)

                # Check if any tasks are actually running
                all_still_pending = all(task.state == 'PENDING' for task in initial_tasks)
                if all_still_pending and time.time() - start_time > 30:
                    print("Tasks remain PENDING for too long. Workers may not be running correctly.")
                    print("Check Redis connection and worker processes.")
                    break

                # Print status every 5 seconds
                current_time = time.time()
                if current_time - last_status_time >= 5:
                    last_status_time = current_time
                    print(f"Active tasks: {active_count}")

                    # Show some task states
                    for i, task in enumerate(initial_tasks[:3]):
                        print(f"Seed task {i+1}: {task.state}")

                # Exit conditions
                if all_initial_done and active_count == 0:
                    print("All tasks completed!")
                    break

                # Short sleep to prevent high CPU usage
                time.sleep(1)

            print("\nCrawler process completed successfully.")

            # Reset buttons on GUI thread
            self.root.after(0, self.reset_buttons)

        except Exception as e:
            print(f"\nError running crawler: {e}")
            import traceback
            print(traceback.format_exc())
            self.root.after(0, self.reset_buttons)


    def reset_buttons(self):
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)

    def stop_crawler(self):
        # Revoke remaining tasks
        if self.monitoring_paused:
            self.toggle_monitor_pause()
        if hasattr(self, 'task_ids') and self.task_ids:
            print("Stopping crawler, revoking remaining tasks...")
            for task_id in self.task_ids:
                try:
                    # Try to revoke the task if it's still active
                    AsyncResult(task_id, app=app).revoke(terminate=True)
                except Exception as e:
                    print(f"Error revoking task {task_id}: {e}")



        # Reset the UI
        self.reset_buttons()
        print("Crawler stopped by user.")

    def on_closing(self):
        # Restore stdout
        sys.stdout = sys.__stdout__

        # Stop the redirector thread if it exists
        if hasattr(self, 'redirector'):
            self.redirector.running = False

        # Stop crawler if running
        self.stop_crawler()

        # Stop worker processes if running
        if hasattr(self, 'worker_process') and self.worker_process:
            try:
                self.worker_process.terminate()
            except:
                pass

        # Close the window
        self.root.destroy()

    def ensure_workers_running(self):
        """Make sure Celery workers are running"""
        print("Checking if Celery workers are running...")

        # Simple check if Redis is available
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            r.ping()
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            print("Make sure Redis server is running!")
            return False

        # Start worker processes
        self.worker_process = subprocess.Popen(
            ["python", "run_workers.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )

        # Rest of your function remains the same...
        worker_ready = {"crawler": False, "indexer": False}

        def read_output():
            for line in iter(self.worker_process.stdout.readline, ''):
                print(f"Worker output: {line.strip()}")
                # Look for successful worker startup indicator
                if "crawler@" in line and "ready" in line:
                    worker_ready["crawler"] = True
                    print("Crawler worker started successfully!")
                if "indexer@" in line and "ready" in line:
                    worker_ready["indexer"] = True
                    print("Indexer worker started successfully!")

        output_thread = threading.Thread(target=read_output)
        output_thread.daemon = True
        output_thread.start()

        print("Started worker processes...")

        # Wait for workers to be ready
        start_time = time.time()
        max_wait = 30  # Wait up to 30 seconds for workers

        while time.time() - start_time < max_wait:
            if worker_ready["crawler"] and worker_ready["indexer"]:
                print("All workers are ready!")
                return True
            time.sleep(1)

        # Continue anyway if at least the crawler is ready
        if worker_ready["crawler"]:
            print("Crawler worker is ready. Continuing.")
            return True

        print("Warning: Couldn't confirm workers are ready. Continuing anyway...")
        return True

def main():
    root = tk.Tk()
    app = CrawlerGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()