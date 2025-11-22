import subprocess
import time
import sys
import requests

def wait_for_server():
    """Waits for the server to be ready."""
    print("Awaiting server...")
    url = "http://localhost:8000/api/search?q=test"
    for _ in range(30):
        try:
            if requests.get(url).status_code in [200, 400]:
                print("Server ready!")
                return
        except:
            pass
        time.sleep(1)
        print(".", end="", flush=True)

def main():
    print("STARTING OPTIMIZED SETUP")
    try:
        subprocess.check_call(["docker-compose", "up", "-d", "online"])
    except subprocess.CalledProcessError:
        print("Error starting Docker services.")
        sys.exit(1)

    wait_for_server()

    print("\n" + "=" * 40)
    print("Site available locally at http://127.0.0.1:8000")
    print("="*40)

if __name__ == "__main__":
    main()