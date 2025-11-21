import subprocess
import sys

def run_offline_task(script_name):
    """Helper to run a script in offline mode."""
    print(f"Running offline task: {script_name}...")
    cmd = [
        "docker compose", "run", "--rm",
        "offline",
        "python", f"scripts/{script_name}"
    ]
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        print("Error running script.")
        sys.exit(1)

def main():
    print("STARTING OFFLINE TASKS")

    print("Checking services...")
    subprocess.check_call(["docker compose", "up", "-d", "elasticsearch"])

    run_offline_task("download_books.py")
    run_offline_task("index_to_elasticsearch.py")
    run_offline_task("build_graphs.py")

    print("ALL DONE!")

if __name__ == "__main__":
    main()