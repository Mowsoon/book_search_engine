import os
import json
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
from elasticsearch_dsl import Document, Text, Integer, Keyword, connections
from multiprocessing import Pool
import numpy as np

try:
    import config
except ImportError:
    from scripts import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BookDocument(Document):
    """Elasticsearch mapping definition."""
    gutenberg_id = Integer()
    title = Text(analyzer='standard')
    author = Text(analyzer='standard')
    image_url = Keyword()
    content = Text(analyzer='standard')

    class Index:
        name = config.ELASTIC["index_name"]
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }


def init_elasticsearch():
    """Creates the index if it does not exist."""
    connections.create_connection(hosts=[config.ELASTIC["host"]])

    if not BookDocument._index.exists():
        logger.info(f"Creating index '{config.ELASTIC['index_name']}'...")
        BookDocument.init()
    else:
        logger.info(f"Index '{config.ELASTIC['index_name']}' already exists.")


def get_indexed_ids(client):
    """Retrieves all book IDs currently stored in the Elasticsearch index."""
    index_name = config.ELASTIC["index_name"]

    if not client.indices.exists(index=index_name):
        return set()

    logger.info("Scanning existing document IDs from Elasticsearch...")
    scanner = scan(
        client,
        index=index_name,
        query={"query": {"match_all": {}}, "_source": False}
    )

    existing_ids = set()
    for hit in scanner:
        try:
            existing_ids.add(int(hit['_id']))
        except (ValueError, TypeError):
            continue

    return existing_ids


def load_book_content(book_id):
    """Reads book content from disk."""
    file_path = os.path.join(config.PATHS["books"], f"{book_id}.txt")
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
        "_index": config.ELASTIC["index_name"],
        "_id": meta.get("id"),
        "_source": {
            "gutenberg_id": meta.get("id"),
            "title": meta.get("title"),
            "author": meta.get("author"),
            "image_url": meta.get("image_url"),
            "content": content
        }
    }


def worker_index_batch(books_subset):
    """
    Worker function: Indexes a batch of books.
    Each worker creates its own ES client connection.
    """
    # Create a dedicated client for this process
    es = Elasticsearch(
        hosts=[config.ELASTIC["host"]],
        request_timeout=config.ELASTIC["timeout"]
    )

    docs = []
    for meta in books_subset:
        book_id = meta.get("id")
        content = load_book_content(book_id)

        if not content:
            continue

        docs.append(create_doc(meta, content))

    if docs:
        # Bulk insert this batch
        success, failed = bulk(
            es,
            docs,
            stats_only=True,
            chunk_size=config.ELASTIC["bulk_chunk_size"]
        )
        return success, failed
    return 0, 0


def run_indexing():
    """Main indexing routine with parallelism."""
    # 1. Setup
    init_elasticsearch()
    es = Elasticsearch(hosts=[config.ELASTIC["host"]])

    metadata_file = config.PATHS["metadata"]
    if not os.path.exists(metadata_file):
        logger.error("Metadata file missing. Run download first.")
        return

    # 2. Load Metadata & Check Existing
    with open(metadata_file, "r", encoding="utf-8") as f:
        all_books = json.load(f)

    indexed_ids = get_indexed_ids(es)
    logger.info(f"Found {len(indexed_ids)} books already indexed.")

    new_books = [b for b in all_books if b.get('id') not in indexed_ids]

    if not new_books:
        logger.info("All books are up to date. Nothing to index.")
        return

    logger.info(f"Starting parallel indexing for {len(new_books)} new books...")

    # 3. Parallel Execution
    workers = config.WORKERS.io_intensive

    num_chunks = workers * 2
    if len(new_books) < num_chunks:
        num_chunks = 1

    chunks = np.array_split(new_books, num_chunks)

    logger.info(f"Dispatching to {workers} workers (processing {num_chunks} batches)...")

    total_success = 0
    total_failed = 0

    with Pool(processes=workers) as pool:
        results = pool.map(worker_index_batch, chunks)

    for s, f in results:
        total_success += s
        total_failed += f

    logger.info(f"Indexing finished. Success: {total_success}, Failed: {total_failed}")


if __name__ == "__main__":
    run_indexing()