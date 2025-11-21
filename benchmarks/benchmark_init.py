import time
import os
import subprocess
import sys

# Ajout du path pour config (nécessaire pour lire les tailles de fichiers)
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
    return total_size / (1024 * 1024)


def run_docker_task(task_name, script_path):
    """Runs a script INSIDE the offline container via Docker Compose."""
    print(f"\n>>> Starting Step: {task_name}")
    start_time = time.time()

    cmd = [
        "docker", "compose", "run", "--rm",
        "offline",
        "python", script_path
    ]

    try:
        # On lance la commande Docker
        subprocess.check_call(cmd, cwd=config.PROJECT_ROOT)
        success = True
    except subprocess.CalledProcessError:
        print(f"!!! Error in step {task_name}")
        success = False

    duration = time.time() - start_time
    print(f"<<< Finished {task_name} in {duration:.2f} seconds.")
    return success, duration


def main():
    print("🤖 STARTING DOCKERIZED BENCHMARK 🤖")

    results = []

    # 1. Initial State
    print("Ensuring Elasticsearch is up...")
    subprocess.check_call(["docker", "compose", "up", "-d", "elasticsearch"], cwd=config.PROJECT_ROOT)
    # Petite pause pour laisser ES démarrer (le healthcheck du compose gère le reste)
    time.sleep(5)

    # 2. Indexing Benchmark
    ok, t_idx = run_docker_task("Indexing (ES)", "scripts/index_to_elasticsearch.py")
    if ok: results.append(("Indexing", t_idx))

    # 3. Graph Benchmark
    # Note: Docker Compose gère les limites RAM (16Go) définies dans le YAML
    ok, t_graph = run_docker_task("Graph Calculation", "scripts/build_graphs.py")
    if ok: results.append(("Graph Build", t_graph))

    # 4. Report
    final_size = get_dir_size(config.DATA_DIR)

    print("\n" + "=" * 40)
    print("       BENCHMARK REPORT (DOCKER)")
    print("=" * 40)
    total_time = 0
    for name, duration in results:
        print(f"{name:<20} : {duration:>8.2f} s")
        total_time += duration

    print("-" * 40)
    print(f"{'Total Time':<20} : {total_time:>8.2f} s")
    print(f"{'Data Size':<20} : {final_size:>8.2f} MB")
    print("=" * 40)


if __name__ == "__main__":
    main()