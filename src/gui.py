#!/usr/bin/env python3
"""
Graphical UI for the distributed web crawler
This script provides a GUI interface for configuring and launching the crawler
"""
import os
import sys
import json
import subprocess
import logging
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def validate_url(url):
    """Simple URL validation"""
    if not url.startswith(('http://', 'https://')):
        return False
    return True

class CrawlerGUI:
    def __init__(self, root):
        """Initialize the GUI"""
        self.root = root
        self.root.title("Distributed Web Crawler")
        self.root.geometry("800x600")

        # Set up the main frame with padding
        main_frame = ttk.Frame(root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Title label
        title_label = ttk.Label(
            main_frame,
            text="Distributed Web Crawler Configuration",
            font=("Arial", 16)
        )
        title_label.pack(pady=10)

        # Create tabs
        tab_control = ttk.Notebook(main_frame)

        # Configuration tab
        config_tab = ttk.Frame(tab_control)
        tab_control.add(config_tab, text="Configuration")

        # Output tab
        output_tab = ttk.Frame(tab_control)
        tab_control.add(output_tab, text="Output")

        tab_control.pack(expand=True, fill="both")

        # Configuration tab layout
        self.setup_config_tab(config_tab)

        # Output tab layout
        self.setup_output_tab(output_tab)

        # Action buttons (at the bottom of the main frame)
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)

        self.start_button = ttk.Button(
            button_frame,
            text="Start Crawling",
            command=self.start_crawling
        )
        self.start_button.pack(side=tk.RIGHT, padx=5)

        self.reset_button = ttk.Button(
            button_frame,
            text="Reset",
            command=self.reset_form
        )
        self.reset_button.pack(side=tk.RIGHT, padx=5)

        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = ttk.Label(
            root,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def setup_config_tab(self, parent):
        """Set up the configuration tab"""
        # Seed URLs section
        seed_frame = ttk.LabelFrame(parent, text="Seed URLs")
        seed_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Help text
        ttk.Label(
            seed_frame,
            text="Enter URLs to start crawling from (one per line). Must start with http:// or https://"
        ).pack(anchor=tk.W, padx=5)

        # Text area for seed URLs
        self.seed_urls_text = scrolledtext.ScrolledText(seed_frame, height=5)
        self.seed_urls_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Default URLs
        default_urls = "https://example.com\nhttps://www.wikipedia.org\nhttps://www.github.com"
        self.seed_urls_text.insert(tk.END, default_urls)

        # Restricted URLs section
        restricted_frame = ttk.LabelFrame(parent, text="Restricted URLs/Domains")
        restricted_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Help text
        ttk.Label(
            restricted_frame,
            text="Enter URLs or domains to avoid crawling (one per line, e.g. facebook.com)"
        ).pack(anchor=tk.W, padx=5)

        # Text area for restricted URLs
        self.restricted_urls_text = scrolledtext.ScrolledText(restricted_frame, height=5)
        self.restricted_urls_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Parameters section
        params_frame = ttk.LabelFrame(parent, text="Crawl Parameters")
        params_frame.pack(fill=tk.BOTH, padx=10, pady=5)

        # Parameters grid
        ttk.Label(params_frame, text="Maximum crawl depth:").grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=5
        )

        self.depth_var = tk.StringVar(value="3")
        depth_spinner = ttk.Spinbox(
            params_frame,
            from_=1, to=10,
            textvariable=self.depth_var,
            width=5
        )
        depth_spinner.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(params_frame, text="Maximum URLs per domain:").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5
        )

        self.max_urls_var = tk.StringVar(value="50")
        max_urls_spinner = ttk.Spinbox(
            params_frame,
            from_=1, to=1000,
            textvariable=self.max_urls_var,
            width=5
        )
        max_urls_spinner.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(params_frame, text="Number of processes:").grid(
            row=2, column=0, sticky=tk.W, padx=5, pady=5
        )

        self.processes_var = tk.StringVar(value="4")
        processes_spinner = ttk.Spinbox(
            params_frame,
            from_=4, to=16,
            textvariable=self.processes_var,
            width=5
        )
        processes_spinner.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        # Help text for processes
        ttk.Label(
            params_frame,
            text="(Minimum 4: 1 master + 2 crawlers + 1 indexer)",
            font=("Arial", 8)
        ).grid(row=2, column=2, sticky=tk.W)

    def setup_output_tab(self, parent):
        """Set up the output tab"""
        # Create output text area with scrollbar
        self.output_text = scrolledtext.ScrolledText(
            parent,
            wrap=tk.WORD,
            background='black',
            foreground='white',
            font=("Consolas", 10)
        )
        self.output_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.output_text.insert(tk.END, "Output will appear here when crawling starts...\n")
        self.output_text.config(state=tk.DISABLED)  # Make read-only

    def get_config(self):
        """Get configuration from UI inputs"""
        # Get and validate seed URLs
        seed_urls_input = self.seed_urls_text.get("1.0", tk.END).strip()
        seed_urls = []
        invalid_urls = []

        for url in seed_urls_input.split("\n"):
            url = url.strip()
            if url:  # Skip empty lines
                if validate_url(url):
                    seed_urls.append(url)
                else:
                    invalid_urls.append(url)

        if invalid_urls:
            messagebox.showwarning(
                "Invalid Seed URLs",
                f"The following URLs are invalid and will be ignored:\n" +
                "\n".join(invalid_urls) +
                "\n\nURLs must start with http:// or https://"
            )

        if not seed_urls:  # Use defaults if no valid URLs
            seed_urls = ["https://example.com", "https://www.wikipedia.org", "https://www.github.com"]
            messagebox.showinfo(
                "Using Default URLs",
                f"No valid URLs entered. Using defaults:\n" + "\n".join(seed_urls)
            )

        # Get restricted URLs/domains
        restricted_urls_input = self.restricted_urls_text.get("1.0", tk.END).strip()
        restricted_urls = [url.strip() for url in restricted_urls_input.split("\n") if url.strip()]

        # Get numeric parameters
        try:
            max_depth = max(1, int(self.depth_var.get()))
        except ValueError:
            max_depth = 3  # Default

        try:
            max_urls_per_domain = max(1, int(self.max_urls_var.get()))
        except ValueError:
            max_urls_per_domain = 50  # Default

        try:
            num_processes = max(4, int(self.processes_var.get()))
        except ValueError:
            num_processes = 4  # Default

        # Create config dict
        config = {
            "seed_urls": seed_urls,
            "restricted_urls": restricted_urls,
            "max_depth": max_depth,
            "max_urls_per_domain": max_urls_per_domain,
            "num_processes": num_processes
        }

        return config

    def reset_form(self):
        """Reset all form fields to defaults"""
        # Reset seed URLs
        self.seed_urls_text.delete("1.0", tk.END)
        self.seed_urls_text.insert(
            tk.END,
            "https://example.com\nhttps://www.wikipedia.org\nhttps://www.github.com"
        )

        # Reset restricted URLs
        self.restricted_urls_text.delete("1.0", tk.END)

        # Reset parameters
        self.depth_var.set("3")
        self.max_urls_var.set("50")
        self.processes_var.set("4")

        # Clear output
        self.output_text.config(state=tk.NORMAL)
        self.output_text.delete("1.0", tk.END)
        self.output_text.insert(tk.END, "Output will appear here when crawling starts...\n")
        self.output_text.config(state=tk.DISABLED)

        # Update status
        self.status_var.set("Form reset to defaults")

    def update_output(self, text):
        """Update the output text area with new text"""
        self.output_text.config(state=tk.NORMAL)
        self.output_text.insert(tk.END, text)
        self.output_text.see(tk.END)  # Auto-scroll to the end
        self.output_text.config(state=tk.DISABLED)

    def start_crawling(self):
        """Start the crawling process with the current configuration"""
        # Get configuration
        config = self.get_config()

        # Show configuration summary
        summary = "\n===== Crawler Configuration Summary =====\n"
        summary += f"Seed URLs ({len(config['seed_urls'])}):\n"
        for url in config['seed_urls']:
            summary += f"  - {url}\n"

        if config['restricted_urls']:
            summary += f"\nRestricted URLs/domains ({len(config['restricted_urls'])}):\n"
            for url in config['restricted_urls']:
                summary += f"  - {url}\n"
        else:
            summary += "\nNo restricted URLs/domains\n"

        summary += f"\nMaximum crawl depth: {config['max_depth']}\n"
        summary += f"Maximum URLs per domain: {config['max_urls_per_domain']}\n"
        summary += f"Number of processes: {config['num_processes']}\n"

        # Clear and update output
        self.output_text.config(state=tk.NORMAL)
        self.output_text.delete("1.0", tk.END)
        self.output_text.insert(tk.END, summary)
        self.output_text.config(state=tk.DISABLED)

        # Ask for confirmation
        if not messagebox.askyesno("Confirm Crawl", "Start crawling with this configuration?"):
            self.status_var.set("Crawl cancelled")
            return

        # Update status
        self.status_var.set("Starting crawler...")
        self.start_button.config(state=tk.DISABLED)

        # Make sure main.py exists
        main_path = os.path.join(os.path.dirname(__file__), "main.py")
        if not os.path.exists(main_path):
            from ui import create_main_script
            create_main_script(main_path)

        # Start the crawler in a separate thread to avoid blocking the UI
        import threading
        threading.Thread(target=self.run_crawler, args=(config,), daemon=True).start()

    def run_crawler(self, config):
        """Run the crawler in a background thread"""
        try:
            # Save the configuration to a temporary file
            with open('crawler_config.json', 'w') as f:
                json.dump(config, f)

            # Build the MPI command
            command = [
                "mpiexec",
                "-n", str(config["num_processes"]),
                "python", os.path.join(os.path.dirname(__file__), "main.py"),
                "--config", "crawler_config.json"
            ]

            # Update output with command
            self.root.after(0, self.update_output, "\nLaunching the distributed web crawler...\n")
            self.root.after(0, self.update_output, f"Command: {' '.join(command)}\n\n")

            # Execute the command
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            # Stream the output
            self.root.after(0, self.update_output, "===== Crawler Output =====\n")

            for line in process.stdout:
                self.root.after(0, self.update_output, line)

            # Wait for process to complete
            return_code = process.wait()

            # Update UI based on result
            if return_code == 0:
                self.root.after(0, self.update_output, "\nCrawl completed successfully!\n")
                self.root.after(0, self.status_var.set, "Crawl completed successfully")
            else:
                self.root.after(0, self.update_output, f"\nCrawl exited with error code: {return_code}\n")
                self.root.after(0, self.status_var.set, f"Crawl failed with code {return_code}")

            # Re-enable the start button
            self.root.after(0, self.start_button.config, {"state": tk.NORMAL})

        except Exception as e:
            error_message = f"Error launching crawler: {e}\n"
            self.root.after(0, self.update_output, error_message)
            self.root.after(0, self.status_var.set, "Error launching crawler")
            self.root.after(0, self.start_button.config, {"state": tk.NORMAL})
            logging.error(error_message)


def main():
    """Main entry point for the GUI application"""
    root = tk.Tk()
    app = CrawlerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()