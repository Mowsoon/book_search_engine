# scripts/index_to_elasticsearch.py
import os
import json
import logging

from django.contrib.messages import success
from django.utils.translation.template import context_re
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Document, Text, Integer, Keyword, connections

try:
    import config
except ImportError:
    from scripts import config


#Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BookDocument(Document):
    """
    Elasticsearch mapping definition.
    """
    gutenberg_id = Integer()
    # Standard analyzer allows simple search AND regex on terms
    title       = Text(analyzer="standard")
    author      = Text(analyzer="standard")
    content     = Text(analyzer="standard")
    image_url   = Keyword()

    class Index:
        name = config.ES_INDEX_NAME
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }


def init_elasticsearch():
    """
    Creates the Elasticsearch index if it doesn't exist.
    """
    connections.create_connection(hosts=[config.ES_HOST])
    if not BookDocument._index.exists():
        logger.info(f"Creating Elasticsearch index '{config.ES_INDEX_NAME}'...")
        BookDocument.init()
    else:
        logger.info(f"Elasticsearch index '{config.ES_INDEX_NAME}' already exists.")

def load_book_content(book_id):
    """Reads book content from disk."""
    file_path = os.path.join(config.BOOKS_DIR, f"{book_id}.txt")
    # Check if file exists before trying to open
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


def generate_books_docs(metadata):
    """Yields document dictionaries for bulk indexing."""
    for i, meta in enumerate(metadata):
        book_id = meta.get("id")
        content = load_book_content(book_id)

        if not content:
            continue

        yield create_doc(meta, content)

        if (i + 1) % 100 == 0:
            logger.info(f"Processed {i + 1} books.")


def run_indexing():
    """Main indexing routine."""
    # 1/ Set up Elasticsearch
    init_elasticsearch()
    es = Elasticsearch(hosts=[config.ES_HOST])

    if not os.path.exists(config.METADATA_FILE):
        logger.error(f"Metadata file '{config.METADATA_FILE}' does not exist. Run download_books.py first.")
        return

    # 2/ Load metadata
    try:
        with open(config.METADATA_FILE, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except json.JSONDecodeError:
        logger.error(f"Metadata file '{config.METADATA_FILE}' is corrupted. Run download_books.py first.")

    logger.info(f"Indexing {len(metadata)} books...")

    # 3/ Run Bulk Indexing
    successful_index_count, failed_index_count = bulk(es, generate_books_docs(metadata), stats_only=True)

    logger.info(f"Indexed {successful_index_count} books successfully.\nFailed indexing: {failed_index_count}")


if __name__ == "__main__":
    run_indexing()