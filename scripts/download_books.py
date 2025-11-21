import os
import time
import json
import requests
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import config
except ImportError:
    from scripts import config


def get_robust_session():
    """Creates a requests session with automatic retry logic from config."""
    session = requests.Session()
    retry_strategy = Retry(
        total=config.NETWORK["retry_total"],
        backoff_factor=config.NETWORK["backoff_factor"],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def book_exists_on_disk(book_id):
    """Checks if the book file already exists locally."""
    filename = os.path.join(config.PATHS["books"], f"{book_id}.txt")
    return os.path.exists(filename) and os.path.getsize(filename) > 0


def save_book_to_disk(book_id, text_content):
    """Writes the book content to a text file."""
    filename = os.path.join(config.PATHS["books"], f"{book_id}.txt")
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(text_content)
        return True
    except IOError as e:
        print(f"[ERROR] Could not save book {book_id}: {e}")
        return False


def get_text_url(book_data):
    """Extracts the text URL from formats."""
    formats = book_data.get("formats", {})
    return (formats.get("text/plain; charset=utf-8") or
            formats.get("text/plain; charset=us-ascii") or
            formats.get("text/plain"))


def process_book_task(book_data):
    """
    Worker task: Checks existence, downloads if needed, validates constraint.
    """
    book_id = book_data.get("id")

    # 1. Fast path: Check existing
    if book_exists_on_disk(book_id):
        authors = book_data.get("authors", [])
        return {
            "id": book_id,
            "title": book_data.get("title"),
            "author": authors[0].get("name") if authors else "Unknown",
            "image_url": book_data.get("formats", {}).get("image/jpeg"),
            "gutenberg_id": book_id,
            "status": "exist"
        }

    # 2. Get URL
    url = get_text_url(book_data)
    if not url:
        return None

    # 3. Download with robust session
    session = get_robust_session()

    try:
        resp = session.get(url, timeout=config.NETWORK["timeout"])

        # Even with retries, we might get a 429 if we exhausted attempts
        if resp.status_code == 429:
            return None

        resp.encoding = 'utf-8'
        text = resp.text

        # 4. Constraint Check
        word_count = len(text.split())
        if word_count < config.CONSTRAINTS["min_words_per_book"]:
            return None

        # 5. Save
        if save_book_to_disk(book_id, text):
            authors = book_data.get("authors", [])
            return {
                "id": book_id,
                "title": book_data.get("title"),
                "author": authors[0].get("name") if authors else "Unknown",
                "image_url": book_data.get("formats", {}).get("image/jpeg"),
                "gutenberg_id": book_id,
                "word_count": word_count,
                "status": "downloaded"
            }

    except Exception:
        return None

    return None

def clean_orphans(valid_metadata):
    """
    Removes .txt files that are not present in the metadata.
    Ensures consistency between disk and index.
    """
    print("\n🧹 Starting final cleanup of orphan files...")

    # Create a set of valid IDs for O(1) lookup
    valid_ids = {str(book['id']) for book in valid_metadata}

    files = os.listdir(config.PATHS["books"])
    deleted_count = 0

    for filename in files:
        if not filename.endswith(".txt"):
            continue

        book_id = filename.replace(".txt", "")

        if book_id not in valid_ids:
            file_path = os.path.join(config.PATHS["books"], filename)
            try:
                os.remove(file_path)
                deleted_count += 1
            except OSError:
                pass

    if deleted_count > 0:
        print(f"Cleanup finished. Deleted {deleted_count} orphan files.")
    else:
        print("No orphan files found. Directory is clean.")

def fetch_books():
    """Main execution loop using Optimized Turbo Logic."""
    books_dir = config.PATHS["books"]
    metadata_file = config.PATHS["metadata"]
    target_count = config.CONSTRAINTS["target_books"]
    workers = config.WORKERS.download  # Using the tuned value (13)

    if not os.path.exists(books_dir):
        os.makedirs(books_dir)

    # Load existing metadata for resume
    books_metadata = []
    existing_ids = set()
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                books_metadata = json.load(f)
                existing_ids = {b['id'] for b in books_metadata}
            print(f"Resuming... {len(books_metadata)} books already collected.")
        except:
            print("Metadata corrupted, starting fresh.")

    next_url = config.GUTENDEX_API
    session = get_robust_session()

    print(f"Target: {target_count} books.")
    print(f"Download with {workers} workers:")

    # Future tracking
    all_futures = []
    stop_queuing = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:

        # PHASE 1: Aggressive Queueing (Fill the pipe)
        while not stop_queuing and next_url:

            # Stop fetching pages if we likely have enough tasks
            # We estimate: current_meta + pending_futures >= target
            pending_futures = [f for f in all_futures if not f.done()]
            potential_total = len(books_metadata) + len(pending_futures)

            if potential_total >= target_count + 50:  # Buffer of 50
                # Wait for tasks to complete before queuing more
                time.sleep(1)

                # Check if we actually reached the target
                if len(books_metadata) >= target_count:
                    stop_queuing = True
                    break
                continue

            try:
                # Fetch list page
                resp = session.get(next_url, timeout=10)
                if resp.status_code == 429:
                    print("API List Rate Limit. Waiting 10s...")
                    time.sleep(10)
                    continue

                data = resp.json()
                next_url = data.get("next")
                results = data.get("results", [])

                for book in results:
                    if book.get("id") not in existing_ids:
                        future = executor.submit(process_book_task, book)
                        all_futures.append(future)

                # --- Harvest Results Logic ---
                # We check completed tasks periodically to free memory
                # and update the metadata list
                done_indices = []
                for i, future in enumerate(all_futures):
                    if future.done():
                        done_indices.append(i)
                        res = future.result()
                        if res:
                            books_metadata.append(res)
                            existing_ids.add(res['id'])

                            if len(books_metadata) % 50 == 0:
                                print(f"Progress: {len(books_metadata)} books.")
                                with open(metadata_file, "w", encoding="utf-8") as f:
                                    json.dump(books_metadata, f, indent=4)

                # Remove harvested futures from the list (in reverse order)
                for i in reversed(done_indices):
                    all_futures.pop(i)

                if len(books_metadata) >= target_count:
                    stop_queuing = True
                    break

                # Small sleep between list pages to be polite
                time.sleep(config.NETWORK["batch_sleep"])

            except Exception as e:
                print(f"[ERROR] Page loop: {e}")
                time.sleep(5)

        # PHASE 2: Drain the queue
        print("Finishing remaining downloads...")
        for future in concurrent.futures.as_completed(all_futures):
            if len(books_metadata) >= target_count:
                break
            res = future.result()
            if res:
                books_metadata.append(res)
                existing_ids.add(res['id'])

    # Final save
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(books_metadata, f, indent=4)

    print(f"Done. Total library size: {len(books_metadata)} books.")

    clean_orphans(books_metadata)


if __name__ == "__main__":
    fetch_books()