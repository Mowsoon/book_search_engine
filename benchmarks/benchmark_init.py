import time
import os
import subprocess
import sys
import shutil
import requests

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


def clean_data_directory():
    """
    Selectively cleans production data to force a full re-initialization.
    PRESERVES benchmark results (.csv, .txt logs).
    """
    print("\n🧹 CLEANING LOCAL DATA...")

    # 1. Supprimer le dossier des livres
    books_dir = config.PATHS["books"]
    if os.path.exists(books_dir):
        print(f"   🗑️  Deleting books directory: {books_dir}")
        shutil.rmtree(books_dir)
    os.makedirs(books_dir, exist_ok=True)

    # 2. Supprimer les fichiers de production spécifiques
    files_to_delete = [
        config.PATHS["metadata"],
        config.PATHS["graph_csv"],
        config.PATHS["ranks_csv"]
    ]

    for file_path in files_to_delete:
        if os.path.exists(file_path):
            print(f"   🗑️  Deleting {os.path.basename(file_path)}")
            os.remove(file_path)


def wipe_docker_volume():
    """
    Stops Docker and removes the volume to force empty Elasticsearch.
    """
    print("\n🧹 WIPING ELASTICSEARCH VOLUME...")
    try:
        # -v removes named volumes declared in the `volumes` section of the Compose file
        subprocess.check_call(["docker", "compose", "down", "-v"], cwd=config.PROJECT_ROOT)
        print("   ✅ Docker volume 'es_data' removed.")
    except subprocess.CalledProcessError:
        print("   ❌ Error wiping Docker volume.")
        sys.exit(1)


def wait_for_elasticsearch():
    """Waits for ES to be responsive."""
    url = config.ELASTIC["host"]
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


def run_docker_task(task_name, script_path):
    """Runs a script INSIDE the offline container via Docker Compose."""
    print(f"\n>>> Starting: {task_name}")
    start_time = time.time()

    cmd = [
        "docker", "compose", "run", "--rm",
        "offline",
        "python", script_path
    ]

    try:
        subprocess.check_call(cmd, cwd=config.PROJECT_ROOT)
        success = True
    except subprocess.CalledProcessError:
        print(f"!!! Error in {task_name}")
        success = False

    duration = time.time() - start_time
    print(f"<<< Finished {task_name} in {duration:.2f} seconds.")
    return success, duration


def run_local_command(name, cmd):
    """Runs a docker compose command on the host."""
    print(f"\n>>> Docker Command: {name}")
    try:
        subprocess.check_call(cmd, cwd=config.PROJECT_ROOT)
    except subprocess.CalledProcessError:
        print(f"❌ Failed: {name}")
        sys.exit(1)


def main():
    print("🤖 STARTING FULL INITIALIZATION BENCHMARK (TRUE FRESH START) 🤖")

    # 0. HARD RESET
    # On nettoie les fichiers locaux
    clean_data_directory()
    # On nettoie la base de données (Volume Docker)
    wipe_docker_volume()

    results = []

    # 1. START INFRASTRUCTURE
    # Le 'up' va recréer le volume vide
    run_local_command("Start Infrastructure", ["docker", "compose", "up", "-d", "elasticsearch"])

    if not wait_for_elasticsearch():
        sys.exit(1)

    # 2. DOWNLOAD (I/O Bound - Network)
    ok, t_dl = run_docker_task("Download Books", "scripts/download_books.py")
    if ok: results.append(("Download", t_dl))

    # 3. INDEXING (I/O Bound - Disk/Network)
    # Cette fois, l'index n'existe plus, donc l'indexation se fera vraiment !
    ok, t_idx = run_docker_task("Indexing to ES", "scripts/index_to_elasticsearch.py")
    if ok: results.append(("Indexing", t_idx))

    # 4. GRAPH BUILDING (CPU/RAM Bound)
    ok, t_graph = run_docker_task("Graph Calculation", "scripts/build_graphs.py")
    if ok: results.append(("Graph Build", t_graph))

    # 5. REPORT
    final_size = get_dir_size(config.PATHS["data"])

    print("\n" + "=" * 50)
    print("       FULL INITIALIZATION REPORT")
    print("=" * 50)
    total_time = 0
    for name, duration in results:
        print(f"{name:<20} : {duration:>8.2f} s  ({duration / 60:.1f} min)")
        total_time += duration

    print("-" * 50)
    print(f"{'Total Time':<20} : {total_time:>8.2f} s  ({total_time / 60:.1f} min)")
    print(f"{'Final Data Size':<20} : {final_size:>8.2f} MB")
    print("=" * 50)

    # Save report
    report_path = os.path.join(config.PATHS["data"], "full_init_benchmark.txt")
    with open(report_path, "w") as f:
        f.write("FULL INITIALIZATION BENCHMARK (True Fresh Start)\n")
        f.write("----------------------------------------------\n")
        for name, duration in results:
            f.write(f"{name}: {duration:.2f}s\n")
        f.write("----------------------------------------------\n")
        f.write(f"Total Time: {total_time:.2f}s ({total_time / 60:.1f} min)\n")
        f.write(f"Data Size: {final_size:.2f}MB\n")

    print(f"📝 Report saved to {report_path}")


if __name__ == "__main__":
    main()