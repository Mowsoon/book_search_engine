# scripts/download_books.py
import os
import time
import json
import requests

# Try to import config from local or module context
try:
    import config
except ImportError:
    from scripts import config


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

    if not text_url:
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


def download_single_book(book_data):
    """Downloads and validates a single book."""
    meta, url = extract_metadata(book_data)
    if not meta or not url:
        return None

    try:
        resp = requests.get(url, timeout=10)
        resp.encoding = 'utf-8'
        text = resp.text

        # Check word count constraint (>= 10,000 words)
        word_count = len(text.split())
        if word_count < config.MIN_WORD_COUNT:
            return None

        if save_book_to_disk(meta['id'], text):
            meta['word_count'] = word_count
            return meta

    except Exception as e:
        print(f"[WARN] Failed to download {url}: {e}")

    return None


def fetch_books():
    """Main execution loop."""
    if not os.path.exists(config.BOOKS_DIR):
        os.makedirs(config.BOOKS_DIR)

    books_metadata = []
    next_url = config.GUTENDEX_API
    count = 0

    print(f"Starting. Target: {config.TARGET_BOOK_COUNT} books.")

    while next_url and count < config.TARGET_BOOK_COUNT:
        try:
            resp = requests.get(next_url, timeout=10)
            data = resp.json()
            next_url = data.get("next")

            for book in data.get("results", []):
                if count >= config.TARGET_BOOK_COUNT:
                    break

                saved_meta = download_single_book(book)
                if saved_meta:
                    books_metadata.append(saved_meta)
                    count += 1
                    if count % 50 == 0:
                        print(f"Progress: {count} books.")

            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"[ERROR] Loop failed: {e}")
            break

    with open(config.METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(books_metadata, f, indent=4)
    print(f"Done. {len(books_metadata)} books saved.")


if __name__ == "__main__":
    fetch_books()