import os
import sys
import subprocess
import time
import platform
import shutil

# --- Configuration ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
VENV_DIR = os.path.join(PROJECT_ROOT, "venv")
IS_WINDOWS = platform.system().lower() == "windows"

# Define paths to executables inside the venv
if IS_WINDOWS:
    PYTHON_EXEC = os.path.join(VENV_DIR, "Scripts", "python.exe")
    PIP_EXEC = os.path.join(VENV_DIR, "Scripts", "pip.exe")
else:
    PYTHON_EXEC = os.path.join(VENV_DIR, "bin", "python")
    PIP_EXEC = os.path.join(VENV_DIR, "bin", "pip")


def print_step(message):
    print(f"\n{'=' * 50}")
    print(f"🚀 {message}")
    print(f"{'=' * 50}\n")


def run_cmd(command, cwd=PROJECT_ROOT, env=None):
    """Runs a command and exits on failure."""
    try:
        # If running python/pip, ensure we use the one from venv
        if command[0] == "python":
            command[0] = PYTHON_EXEC
        if command[0] == "pip":
            command[0] = PIP_EXEC

        subprocess.check_call(command, cwd=cwd, env=env)
    except subprocess.CalledProcessError:
        print(f"\n❌ Error executing: {' '.join(command)}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
        sys.exit(0)


def setup_environment():
    """Creates venv and installs requirements."""
    print_step("Checking Environment")

    # 1. Create venv if missing
    if not os.path.exists(VENV_DIR):
        print(f"Creating virtual environment at {VENV_DIR}...")
        subprocess.check_call([sys.executable, "-m", "venv", "venv"])
    else:
        print("Virtual environment already exists.")

    # 2. Install requirements
    print("Installing/Updating dependencies...")
    run_cmd(["pip", "install", "-r", "requirements.txt"])


def wait_for_elasticsearch():
    """Checks if ES is reachable."""
    import requests  # Import here because it's installed in step 2
    print("⏳ Waiting for Elasticsearch...")
    url = "http://localhost:9200"
    for _ in range(30):
        try:
            if requests.get(url).status_code == 200:
                print("✅ Elasticsearch is ready!")
                return
        except:
            pass
        time.sleep(1)
        print(".", end="", flush=True)
    print("\n❌ Elasticsearch is not responding. Check Docker logs.")
    sys.exit(1)


def start_services():
    print_step("Starting Docker Services")
    try:
        subprocess.check_call(["docker-compose", "up", "-d"])
    except FileNotFoundError:
        print("❌ Docker is not installed or not in PATH.")
        sys.exit(1)

    # We need to use the venv's python to import requests for the check
    subprocess.check_call([PYTHON_EXEC, "-c",
                           "import requests, time; "
                           "url='http://localhost:9200'; "
                           "print('Waiting for ES...'); "
                           "[(time.sleep(1), print('.', end='', flush=True)) for _ in range(60) if requests.get(url, verify=False).status_code != 200]; "
                           "print('Ready!')"
                           ])


def main():
    # 1. Setup Python Env
    setup_environment()

    print_step("Startup Menu")
    print("1. 🆕 FULL INIT (Download Books + Build Graphs + Index + Run)")
    print("   -> Use this if data folder is empty or corrupted.")
    print("")
    print("2. ⚡ FAST START (Index Check + Run Server)")
    print("   -> Use this if you already have 'data/books' and CSV files.")
    print("   -> It will just update Elasticsearch index if needed.")
    print("")
    print("3. 🏃 JUST RUN (Docker + Server)")
    print("   -> Use this if everything is already running and indexed.")
    print("")

    choice = input("👉 Choose (1/2/3): ").strip()

    # 2. Start Docker (Always needed)
    start_services()

    # 3. Execute Scripts based on choice
    if choice == "1":
        # Full download (Long)
        run_cmd(["python", "scripts/download_books.py"])

        # Heavy processing (RAM intensive)
        print("\n🛑 Stopping Docker temporarily to free RAM for Graph calculation...")
        subprocess.check_call(["docker-compose", "stop"])

        run_cmd(["python", "scripts/build_graphs.py"])

        print("\n🔄 Restarting Docker...")
        subprocess.check_call(["docker-compose", "start"])
        # Wait again for ES
        time.sleep(5)

        # Indexing
        run_cmd(["python", "scripts/index_to_elasticsearch.py"])

    elif choice == "2":
        # Just ensure Index is synced (Fast)
        run_cmd(["python", "scripts/index_to_elasticsearch.py"])

    elif choice == "3":
        pass  # Nothing extra to do

    else:
        print("Invalid choice.")
        sys.exit(1)

    # 4. Start Django Server
    print_step("Starting Django Server")
    print("🌍 Server running at: http://127.0.0.1:8000/")
    print("📱 Connect devices to your local IP: http://192.168.X.X:8000/")
    print("(Ctrl+C to stop)")

    # We run manage.py from the back_end directory context
    run_cmd(["python", "manage.py", "runserver", "0.0.0.0:8000"], cwd=os.path.join(PROJECT_ROOT, "back_end"))


if __name__ == "__main__":
    main()