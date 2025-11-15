import time
import os
import subprocess
import sys

# Add parent dir to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import config


def get_dir_size(path):
    """Calculates total size of a directory in MB."""
    total_size = 0
    if not os.path.exists(path):
        return 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size / (1024 * 1024)  # Convert to MB


def run_step(name, command):
    """Runs a command and measures execution time."""
    print(f"\n>>> Starting Step: {name}")
    start_time = time.time()

    try:
        # Using sys.executable ensures we use the current venv python
        cmd_list = command if isinstance(command, list) else command.split()
        if cmd_list[0] == 'python':
            cmd_list[0] = sys.executable

        subprocess.check_call(cmd_list)
        success = True
    except subprocess.CalledProcessError:
        print(f"!!! Error in step {name}")
        success = False

    duration = time.time() - start_time
    print(f"<<< Finished {name} in {duration:.2f} seconds.")
    return success, duration


def main():
    print("🤖 STARTING FULL INITIALIZATION BENCHMARK 🤖")
    print(f"Target: {config.TARGET_BOOK_COUNT} books")
    print(f"Jaccard Workers: {config.WORKERS_JACCARD}")
    print(f"Closeness Workers: {config.WORKERS_CLOSENESS}")

    results = []

    # Initial Storage Check
    initial_size = get_dir_size(config.DATA_DIR)
    print(f"Initial Data Size: {initial_size:.2f} MB")

    # 1. Download
    # Note: This might be fast if books are already there.
    # For a true test, clean 'data/books' before running.
    ok, t_dl = run_step("Download", "python scripts/download_books.py")
    results.append(("Download", t_dl))

    # 2. Indexing
    ok, t_idx = run_step("Indexing", "python scripts/index_to_elasticsearch.py")
    results.append(("Indexing", t_idx))

    # 3. Graph Building
    # This is the CPU/RAM heavy part
    ok, t_graph = run_step("Graph Build", "python scripts/build_graphs.py")
    results.append(("Graph Calculation", t_graph))

    # Final Report
    final_size = get_dir_size(config.DATA_DIR)
    growth = final_size - initial_size

    print("\n" + "=" * 40)
    print("       BENCHMARK REPORT")
    print("=" * 40)
    total_time = 0
    for name, duration in results:
        print(f"{name:<20} : {duration:>8.2f} s")
        total_time += duration

    print("-" * 40)
    print(f"{'Total Time':<20} : {total_time:>8.2f} s")
    print(f"{'Storage Used':<20} : {final_size:>8.2f} MB")
    print(f"{'Storage Growth':<20} : +{growth:>7.2f} MB")
    print("=" * 40)

    # Save to file for report
    with open("init_benchmark_report.txt", "w") as f:
        f.write(f"Total Time: {total_time:.2f}s\n")
        f.write(f"Storage Used: {final_size:.2f}MB\n")
        f.write(f"Config: Jaccard={config.WORKERS_JACCARD}cpus, Closeness={config.WORKERS_CLOSENESS}cpus\n")


if __name__ == "__main__":
    main()