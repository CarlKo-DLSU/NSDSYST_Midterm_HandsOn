#!/usr/bin/env python3

import json
import datetime
import threading
import argparse
import Pyro5.api

@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class TaskQueue(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.tasks = {}
        self.load_tasks()

    def load_tasks(self):
        """Loads tasks from tasks.json and injects initial status and result keys."""
        try:
            with open("tasks.json", "r") as f:
                raw_tasks = json.load(f)
                with self.lock:
                    for task_id, task_data in raw_tasks.items():
                        task_data["status"] = "pending"
                        task_data["result"] = None
                        self.tasks[task_id] = task_data
                print(f"[{self.get_timestamp()}] [INFO] Loaded {len(self.tasks)} tasks successfully.")
        except FileNotFoundError:
            print(f"[{self.get_timestamp()}] [ERROR] tasks.json not found. Initializing empty queue.")
            self.tasks = {}

    def get_timestamp(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def request_task(self):
        """Finds and claims the first available 'pending' task."""
        with self.lock:
            for task_id, task_data in self.tasks.items():
                if task_data["status"] == "pending":
                    task_data["status"] = "in-progress"
                    print(f"[{self.get_timestamp()}] [INFO] Task {task_id} status updated to in-progress.")
                    return task_id, task_data
            return None

    def submit_result(self, task_id, result, worker_name):
        """Updates the task in local storage and logs execution completion."""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]["status"] = "completed"
                self.tasks[task_id]["result"] = result
                timestamp = self.get_timestamp()
                print(f"[{timestamp}] [SUCCESS] Task <{task_id}> completed by <{worker_name}>. Result: <{result}>")
                return True
            return False

def main():
    print("Initializing Master Server...")
    parser = argparse.ArgumentParser(description="Master Server for Pyro5 task queue")
    parser.add_argument("--ns-host", default="localhost", help="Pyro Name Server host/IP")
    parser.add_argument("--daemon-host", default="localhost", help="Master daemon bind host/IP")
    args = parser.parse_args()

    ns_host = args.ns_host
    daemon_host = args.daemon_host
    print(f"Name Server host: {ns_host}")
    print(f"Daemon binding host: {daemon_host}")
    
    try:
        # Locate the Pyro Name Server
        ns = Pyro5.api.locate_ns(host=ns_host)
        
        # Start daemon and register MasterServer
        daemon = Pyro5.api.Daemon(host=daemon_host)
        uri = daemon.register(TaskQueue)
        ns.register("MasterServer", uri)
        
        print(f"Master Server is active and registered under 'MasterServer'.")
        print("Waiting for worker connections...\n")
        daemon.requestLoop()
    except Exception as e:
        print(f"Initialization Error: {e}")
        print("Ensure the Pyro Name Server is running (pyro5-ns) before starting this script.")

if __name__ == "__main__":
    main()