import datetime
import random
import time
import Pyro5.api

def get_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def perform_math(action, values):
    """Computes arithmetic operations locally based on remote task definition."""
    if not values or len(values) < 2:
        return "Error: Insufficient values"
    
    val1, val2 = values[0], values[1]
    if action == "add":
        return val1 + val2
    elif action == "subtract":
        return val1 - val2
    elif action == "multiply":
        return val1 * val2
    elif action == "divide":
        if val2 == 0:
            return "Error: Division by zero"
        return val1 / val2
    else:
        return f"Error: Unknown action '{action}'"

def main():
    print("=== Worker Node Client ===")
    worker_name = input("Enter worker node name (e.g., Worker_Node_A): ").strip()
    if not worker_name:
        worker_name = f"Worker_{random.randint(1000, 9999)}"
        
    print(f"[{get_timestamp()}] [INFO] Initialized as {worker_name}")
    
    try:
        # Connect to Master Server via Name Server
        ns = Pyro5.api.locate_ns()
        uri = ns.lookup("MasterServer")
        master = Pyro5.api.Proxy(uri)
        print(f"[{get_timestamp()}] [INFO] Connected to MasterServer.")
    except Exception as e:
        print(f"[{get_timestamp()}] [ERROR] Failed to discover MasterServer: {e}")
        return

    try:
        while True:
            # 1. SLEEPING
            sleep_time = random.randint(3, 5)
            print(f"[{get_timestamp()}] [SLEEPING] Pausing for {sleep_time} seconds...")
            time.sleep(sleep_time)

            # 2. FETCHING
            print(f"[{get_timestamp()}] [FETCHING] Requesting task from Master...")
            try:
                task_payload = master.request_task()
            except Exception as e:
                print(f"[{get_timestamp()}] [ERROR] Connection lost or master server unavailable: {e}")
                break

            if task_payload is None:
                print(f"[{get_timestamp()}] [IDLE] No pending tasks available. Waiting...")
                continue

            task_id, task_data = task_payload
            action = task_data.get("action")
            values = task_data.get("values")

            # 3. COMPUTING
            print(f"[{get_timestamp()}] [COMPUTING] Task {task_id}: Performing {action} on {values}...")
            result = perform_math(action, values)

            # 4. SUBMITTING
            print(f"[{get_timestamp()}] [SUBMITTING] Sending result {result} to Master...")
            try:
                master.submit_result(task_id, result, worker_name)
            except Exception as e:
                print(f"[{get_timestamp()}] [ERROR] Result transmission failed: {e}")

    except KeyboardInterrupt:
        print(f"\n[{get_timestamp()}] [INFO] Worker node shutting down.")
    finally:
        master._pyroRelease()

if __name__ == "__main__":
    main()