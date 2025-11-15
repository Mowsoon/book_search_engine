import os
import multiprocessing

# --- Path Configuration ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
BOOKS_DIR = os.path.join(DATA_DIR, "books")
METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")

# --- Gutendex API Configuration ---
GUTENDEX_API = "http://gutendex.com/books/"

# --- Elasticsearch Configuration ---
ES_HOST = 'http://localhost:9200'
ES_INDEX_NAME = 'gutenberg_books'

# --- Project Constraints ---
TARGET_BOOK_COUNT = 1670
MIN_WORD_COUNT = 10000

# --- Graph & Ranking Outputs ---
GRAPH_FILE = os.path.join(DATA_DIR, "book_graph.csv")
RANK_FILE = os.path.join(DATA_DIR, "book_ranks.csv")
JACCARD_THRESHOLD = 0.15

# --- Performance Configuration ---
# 1. Heavy Task (Jaccard Similarity)
# High RAM usage per process (needs book text in memory).
# Limit this to avoid swapping/freezing on Windows.
# We use 6 workers as a safe baseline for 32GB RAM.
WORKERS_JACCARD = min(6, multiprocessing.cpu_count())

# 2. Light Task (Closeness Centrality)
# Low RAM usage (graph structure only), purely CPU bound.
# We can safely use ALL available cores for maximum speed.
WORKERS_CLOSENESS = multiprocessing.cpu_count()