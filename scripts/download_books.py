import os
import time
import json
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import config
except ImportError:
    from scripts import config


def get_robust_session():
    """
    Creates a requests session with automatic retry logic.
    """
    session = requests.Session()
    # Retry 3 times on connection errors or 5xx server errors
    # backoff_factor=1 means: wait 1s, then 2s, then 4s between retries
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def book_exists_on_disk(book_id):
    """Checks if the book file already exists locally."""
    filename = os.path.join(config.BOOKS_DIR, f"{book_id}.txt")
    return os.path.exists(filename) and os.path.getsize(filename) > 0


def save_book_to_disk(book_id, text_content):
    """Writes the book content to a text file."""
    filename = os.path.join(config.BOOKS_DIR, f"{book_id}.txt")
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(text_content)
        return True
    except IOError as e:
        print(f"[ERROR] Could not save book {book_id}: {e}")
        return False


def extract_metadata(book):
    """Parses the API response to extract relevant book info."""
    formats = book.get("formats", {})
    text_url = (formats.get("text/plain; charset=utf-8") or
                formats.get("text/plain; charset=us-ascii") or
                formats.get("text/plain"))

    # Skip check if we already have the book on disk (we might not need URL)
    if not text_url and not book_exists_on_disk(book.get("id")):
        return None, None

    authors = book.get("authors", [])
    author_name = authors[0].get("name") if authors else "Unknown"

    metadata = {
        "id": book.get("id"),
        "title": book.get("title"),
        "author": author_name,
        "image_url": formats.get("image/jpeg"),
        "gutenberg_id": book.get("id")
    }
    return metadata, text_url


def process_single_book(session, book_data):
    """
    Downloads book if needed, using the robust session.
    """
    meta, url = extract_metadata(book_data)
    if not meta:
        return None

    # Optimization: If on disk, skip download
    if book_exists_on_disk(meta['id']):
        return meta

    if not url:
        return None

    try:
        # Use the session to get the book content
        resp = session.get(url, timeout=20)
        resp.encoding = 'utf-8'
        text = resp.text

        # Constraint: Check word count
        word_count = len(text.split())
        if word_count < config.MIN_WORD_COUNT:
            return None

        if save_book_to_disk(meta['id'], text):
            meta['word_count'] = word_count
            return meta

    except Exception as e:
        # Log warning but DO NOT CRASH the script
        print(f"[WARN] Failed to download book {meta['id']}: {e}")

    return None


def fetch_books():
    """Main loop."""
    if not os.path.exists(config.BOOKS_DIR):
        os.makedirs(config.BOOKS_DIR)

    # Init robust session
    session = get_robust_session()

    books_metadata = []
    existing_ids = set()

    # Load existing metadata (Resume capability)
    if os.path.exists(config.METADATA_FILE):
        try:
            with open(config.METADATA_FILE, "r", encoding="utf-8") as f:
                books_metadata = json.load(f)
                existing_ids = {b['id'] for b in books_metadata}
            print(f"Resuming... Loaded {len(books_metadata)} books from metadata.")
        except json.JSONDecodeError:
            print("Metadata corrupted. Starting fresh.")

    next_url = config.GUTENDEX_API
    print(f"Target: {config.TARGET_BOOK_COUNT} books.")

    while next_url and len(books_metadata) < config.TARGET_BOOK_COUNT:
        try:
            # Get the list of books (API page)
            resp = session.get(next_url, timeout=30)
            if resp.status_code != 200:
                print(f"[ERROR] API Error {resp.status_code}. Retrying in 5s...")
                time.sleep(5)
                continue

            data = resp.json()
            next_url = data.get("next")

            for book in data.get("results", []):
                if len(books_metadata) >= config.TARGET_BOOK_COUNT:
                    break

                if book.get("id") in existing_ids:
                    continue

                saved_meta = process_single_book(session, book)

                if saved_meta:
                    books_metadata.append(saved_meta)
                    existing_ids.add(saved_meta['id'])

                    if len(books_metadata) % 10 == 0:
                        print(f"Progress: {len(books_metadata)} books collected.")
                        # Incremental save
                        with open(config.METADATA_FILE, "w", encoding="utf-8") as f:
                            json.dump(books_metadata, f, indent=4)

            # RANDOM SLEEP: Critical to avoid being blocked
            sleep_time = random.uniform(1.0, 3.0)
            time.sleep(sleep_time)

        except Exception as e:
            print(f"[CRITICAL] Main loop error: {e}")
            print("Waiting 10s before trying to resume...")
            time.sleep(10)
            # We don't break, we loop again to retry 'next_url' (or the script restarts)

    # Final save
    with open(config.METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(books_metadata, f, indent=4)

    print(f"Finished. Total library size: {len(books_metadata)} books.")


if __name__ == "__main__":
    fetch_books()