#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import yaml
import os
import subprocess
import threading
import queue
import sys
import time
from crawler_config import CrawlerConfig

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
        self.config_file = "crawler_config.yaml"
        self.process = None

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

    def setup_monitor_tab(self):
        # Create console output
        console_frame = ttk.LabelFrame(self.monitor_tab, text="Crawler Output")
        console_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.console_text = scrolledtext.ScrolledText(console_frame, wrap=tk.WORD, state=tk.DISABLED)
        self.console_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Control buttons
        controls_frame = ttk.Frame(self.monitor_tab)
        controls_frame.pack(fill=tk.X, pady=5)

        ttk.Button(controls_frame, text="Clear Console",
                  command=self.clear_console).pack(side=tk.RIGHT, padx=5)

    def browse_output_dir(self):
        directory = filedialog.askdirectory(initialdir=".", title="Select Output Directory")
        if directory:
            self.output_var.set(directory)

    def clear_console(self):
        self.console_text.config(state=tk.NORMAL)
        self.console_text.delete(1.0, tk.END)
        self.console_text.config(state=tk.DISABLED)

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

        # Calculate total processes needed
        total_processes = 1 + self.config['num_crawlers'] + self.config['num_indexers']

        # Create launcher.py if it doesn't exist
        if not os.path.exists("src/launcher.py"):
            self.create_launcher_file()

        # Redirect stdout to the console text widget
        self.redirector = StdoutRedirector(self.console_text)
        sys.stdout = self.redirector

        # Start the crawler in a separate thread to avoid blocking the GUI
        self.crawler_thread = threading.Thread(
            target=self.run_crawler_process,
            args=(total_processes,)
        )
        self.crawler_thread.daemon = True
        self.crawler_thread.start()

        # Update button states
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)

        # Switch to monitor tab
        self.notebook.select(1)

    def run_crawler_process(self, total_processes):
        try:
            cmd = ["mpiexec", "-n", str(total_processes), "python", "src/launcher.py"]
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )

            # Print command being executed
            print(f"Executing: {' '.join(cmd)}\n")
            print(f"Starting crawler with {self.config['num_crawlers']} crawler(s) and {self.config['num_indexers']} indexer(s)...\n")

            # Read output line by line
            for line in iter(self.process.stdout.readline, ''):
                print(line, end='')

            # Process finished
            self.process.stdout.close()
            return_code = self.process.wait()

            if return_code == 0:
                print("\nCrawler process completed successfully.")
            else:
                print(f"\nCrawler process ended with return code {return_code}.")

            # Reset buttons on GUI thread
            self.root.after(0, self.reset_buttons)

        except Exception as e:
            print(f"\nError running crawler: {e}")
            # Reset buttons on GUI thread
            self.root.after(0, self.reset_buttons)

    def reset_buttons(self):
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)

    def stop_crawler(self):
        if self.process and self.process.poll() is None:
            print("\nStopping crawler process...")
            self.process.terminate()

            # Give it a moment to terminate gracefully
            for _ in range(5):
                if self.process.poll() is not None:
                    break
                time.sleep(0.5)

            # Force kill if still running
            if self.process.poll() is None:
                self.process.kill()
                print("Crawler process forcefully terminated.")

            self.reset_buttons()


    def on_closing(self):
        # Restore stdout
        sys.stdout = sys.__stdout__

        # Stop the redirector thread if it exists
        if hasattr(self, 'redirector'):
            self.redirector.running = False

        # Kill subprocess if running
        self.stop_crawler()

        # Close the window
        self.root.destroy()

def main():
    root = tk.Tk()
    app = CrawlerGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()