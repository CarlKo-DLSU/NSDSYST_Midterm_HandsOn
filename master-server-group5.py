import json
import datetime
import threading
import Pyro5.api

# Configuration - replace these with the actual IP addresses of your network hosts.
NAMESERVER_HOST = "10.20.101.16"  # IP address of the machine running pyro5-ns
NAMESERVER_PORT = 9090
MASTER_HOST = "10.20.101.17"  # IP address of the machine running this master server

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
    try:
        # Locate the Pyro Name Server
        ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        
        # Start daemon and register MasterServer.
        # Use MASTER_HOST so the returned URI is reachable by remote worker nodes.
        daemon = Pyro5.api.Daemon(host=MASTER_HOST)
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