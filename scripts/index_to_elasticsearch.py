# scripts/index_to_elasticsearch.py
import os
import json
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
from elasticsearch_dsl import Document, Text, Integer, Keyword, connections

try:
    import config
except ImportError:
    from scripts import config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BookDocument(Document):
    """Elasticsearch mapping definition."""
    gutenberg_id = Integer()
    title = Text(analyzer='standard')
    author = Text(analyzer='standard')
    image_url = Keyword()
    content = Text(analyzer='standard')

    class Index:
        name = config.ES_INDEX_NAME
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }


def init_elasticsearch():
    """Creates the index if it does not exist."""
    connections.create_connection(hosts=[config.ES_HOST])
    if not BookDocument._index.exists():
        logger.info(f"Creating index '{config.ES_INDEX_NAME}'...")
        BookDocument.init()
    else:
        logger.info(f"Index '{config.ES_INDEX_NAME}' already exists.")


def get_indexed_ids(es_client):
    """
    Retrieves all book IDs currently stored in the Elasticsearch index.
    Returns a set of integers.
    """
    if not es_client.indices.exists(index=config.ES_INDEX_NAME):
        return set()

    logger.info("Fetching existing document IDs from Elasticsearch...")
    # 'scan' is an efficient way to retrieve all documents from an index
    # We only need the _id field, so we disable _source to save bandwidth
    scanner = scan(
        es_client,
        index=config.ES_INDEX_NAME,
        query={"query": {"match_all": {}}, "_source": False}
    )

    existing_ids = set()
    for hit in scanner:
        try:
            existing_ids.add(int(hit['_id']))
        except ValueError:
            continue  # Skip if ID is not an integer

    return existing_ids


def load_book_content(book_id):
    """Reads book content from disk."""
    file_path = os.path.join(config.BOOKS_DIR, f"{book_id}.txt")
    if not os.path.exists(file_path):
        return ""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except IOError:
        return ""


def create_doc(meta, content):
    """Helper to create a document dictionary."""
    return {
        "_index": config.ES_INDEX_NAME,
        "_id": meta.get("id"),
        "_source": {
            "gutenberg_id": meta.get("id"),
            "title": meta.get("title"),
            "author": meta.get("author"),
            "image_url": meta.get("image_url"),
            "content": content
        }
    }


def generate_book_docs(metadata_subset):
    """Yields document dictionaries for bulk indexing."""
    total = len(metadata_subset)
    for i, meta in enumerate(metadata_subset):
        book_id = meta.get("id")
        content = load_book_content(book_id)

        if not content:
            continue

        yield create_doc(meta, content)

        if (i + 1) % 50 == 0:
            logger.info(f"Prepared {i + 1}/{total} books for bulk...")


def run_indexing():
    """Main indexing routine with resume capability."""
    init_elasticsearch()
    es = Elasticsearch(hosts=[config.ES_HOST])

    if not os.path.exists(config.METADATA_FILE):
        logger.error("Metadata file missing. Run download first.")
        return

    # 1. Load Local Metadata
    with open(config.METADATA_FILE, "r", encoding="utf-8") as f:
        all_books_meta = json.load(f)

    # 2. Get Already Indexed IDs
    indexed_ids = get_indexed_ids(es)
    logger.info(f"Found {len(indexed_ids)} books already in index.")

    # 3. Filter: Only keep books that are NOT in the index
    books_to_index = [
        b for b in all_books_meta
        if b.get('id') not in indexed_ids
    ]

    if not books_to_index:
        logger.info("All books are up to date. Nothing to index.")
        return

    logger.info(f"Starting indexing for {len(books_to_index)} new books...")

    # 4. Bulk Indexing
    success, failed = bulk(
        es,
        generate_book_docs(books_to_index),
        stats_only=True,
        chunk_size=50
    )

    logger.info(f"Done. Added: {success}, Failed: {failed}")


if __name__ == "__main__":
    run_indexing()