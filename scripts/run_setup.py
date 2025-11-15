import subprocess
import time
import sys
import requests


def run_command(command, step_name, ignore_error=False):
    """Helper to run a shell command."""
    print(f"\n========================================")
    print(f"STEP: {step_name}")
    print(f"========================================\n")
    try:
        if command[0] == "python":
            command[0] = sys.executable

        # Use shell=True on Windows for some commands if needed, but usually list is safer
        subprocess.check_call(command)
        print(f"\n✅ {step_name}: SUCCESS")
    except subprocess.CalledProcessError:
        print(f"\n❌ {step_name}: FAILED")
        if not ignore_error:
            sys.exit(1)


def wait_for_elasticsearch():
    """Waits for ES to be responsive."""
    url = "http://localhost:9200"
    print("Waiting for Elasticsearch to start...")
    for i in range(60):
        try:
            if requests.get(url).status_code == 200:
                print("✅ Elasticsearch is ready!")
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
        print(".", end="", flush=True)
    print("\n❌ Error: Elasticsearch unreachable.")
    sys.exit(1)


def main():
    print("STARTING OPTIMIZED SETUP")

    # --- PHASE 1: DATA DOWNLOAD & INDEXING (Needs Docker) ---

    # 1. Start Docker
    run_command(["docker-compose", "up", "-d"], "Starting Docker")
    wait_for_elasticsearch()

    # 2. Download (I/O bound, low RAM)
    run_command(["python", "scripts/download_books.py"], "Downloading Books")

    # 3. Indexing (Needs ES)
    run_command(["python", "scripts/index_to_elasticsearch.py"], "Indexing to ES")

    # --- PHASE 2: GRAPH CALCULATION (CPU/RAM Heavy) ---

    # 4. Stop Docker to free RAM [USER OPTIMIZATION]
    print("\n🛑 Stopping Docker to free RAM for graph calculation...")
    run_command(["docker-compose", "stop"], "Stopping Docker Services")

    # 5. Build Graph (High RAM usage on Windows)
    # Note: Ensure GRAPH_BUILD_WORKERS is low in config.py (e.g., 4 or 6)
    run_command(["python", "scripts/build_graphs.py"], "Building Graphs & Ranking")

    # --- PHASE 3: RESTORE SERVICE ---

    # 6. Restart Docker for the API
    print("\n🔄 Restarting Docker for the API...")
    run_command(["docker-compose", "start"], "Restarting Docker")
    wait_for_elasticsearch()

    print("\nALL DONE! PROJECT IS READY.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted.")