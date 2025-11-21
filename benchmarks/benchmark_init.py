import time
import os
import subprocess
import sys  # <-- Assurez-vous que c'est importé
import requests

# --- AJOUT CRUCIAL ---
# Ajoute le dossier racine (projet3/) au chemin de Python
# pour qu'il puisse trouver le "paquet" scripts/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
# --- FIN DE L'AJOUT ---

try:
    from scripts import config
except ImportError as e:
    print(f"Error: Could not import config.py. Path setup failed. {e}")
    sys.exit(1)

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


def run_step(name, command, check_call=True):
    """Runs a command and measures execution time."""
    print(f"\n>>> Starting Step: {name}")
    start_time = time.time()
    success = False
    try:
        # Use sys.executable to ensure we use the venv python
        cmd_list = command if isinstance(command, list) else command.split()
        if cmd_list[0] == 'python':
            cmd_list[0] = sys.executable

        # --- CORRECTION ICI ---
        # Execute the command from the PROJECT_ROOT directory
        subprocess.check_call(cmd_list, cwd=PROJECT_ROOT)

        success = True
    except subprocess.CalledProcessError:
        print(f"!!! Error in step {name}")
    except KeyboardInterrupt:
        print("!!! Step interrupted.")

    duration = time.time() - start_time
    print(f"<<< Finished {name} in {duration:.2f} seconds.")
    return success, duration


def wait_for_elasticsearch():
    """Waits for ES to be responsive."""
    url = config.ES_HOST
    print(f"Waiting for Elasticsearch at {url}...")
    for _ in range(60):  # Try for 60 seconds
        try:
            if requests.get(url, timeout=2).status_code == 200:
                print("✅ Elasticsearch is ready!")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(1)
        print(".", end="", flush=True)
    print("\n❌ Error: Elasticsearch unreachable.")
    return False


def main():
    print("🤖 STARTING INITIALIZATION BENCHMARK 🤖")
    print("(Assuming data/books and metadata.json already exist)")
    print(f"Jaccard Workers: {config.WORKERS_JACCARD}")
    print(f"Closeness Workers: {config.WORKERS_CLOSENESS}")

    results = []
    t_idx, t_graph = 0.0, 0.0  # Init timers

    # --- 1. Indexing (Needs Docker) ---
    run_step("Starting Docker", ["docker-compose", "up", "-d"])
    if not wait_for_elasticsearch():
        sys.exit(1)

    ok, t_idx = run_step("Indexing (ES)", ["python", "scripts/index_to_elasticsearch.py"])
    if ok:
        results.append(("Indexing (ES)", t_idx))

    # --- 2. Graph (No Docker) ---
    print("\n🛑 Stopping Docker to free RAM for graph calculation...")
    run_step("Stopping Docker", ["docker-compose", "stop"], check_call=False)

    ok, t_graph = run_step("Graph Build (Jaccard + PageRank)", ["python", "scripts/build_graphs.py"])
    if ok:
        results.append(("Graph Calculation", t_graph))

    # --- 3. Report ---
    final_size = get_dir_size(config.DATA_DIR)

    print("\n" + "=" * 40)
    print("       INITIALIZATION BENCHMARK REPORT")
    print("=" * 40)
    total_time = 0
    for name, duration in results:
        print(f"{name:<20} : {duration:>8.2f} s")
        total_time += duration

    print("-" * 40)
    print(f"{'Total Compute Time':<20} : {total_time:>8.2f} s")
    print(f"{'Data Storage Size':<20} : {final_size:>8.2f} MB")
    print("=" * 40)

    # Save to file for report
    report_file = os.path.join(config.DATA_DIR, "init_benchmark_report.txt")
    with open(report_file, "w") as f:
        f.write(f"Total Compute Time: {total_time:.2f}s\n")
        f.write(f"Indexing Time: {t_idx:.2f}s\n")
        f.write(f"Graph Time: {t_graph:.2f}s\n")
        f.write(f"Data Storage: {final_size:.2f}MB\n")
        f.write(f"Config: Jaccard={config.WORKERS_JACCARD}cpus, Closeness={config.WORKERS_CLOSENESS}cpus\n")
    print(f"Report saved to {report_file}")


if __name__ == "__main__":
    main()