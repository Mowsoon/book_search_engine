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
ES_HOST = os.environ.get('ES_HOST', 'http://localhost:9200')
ES_INDEX_NAME = 'gutenberg_books'

# --- Project Constraints ---
TARGET_BOOK_COUNT = 1670
MIN_WORD_COUNT = 10000

# --- Graph & Ranking Outputs ---
GRAPH_FILE = os.path.join(DATA_DIR, "book_graph.csv")
RANK_FILE = os.path.join(DATA_DIR, "book_ranks.csv")
JACCARD_THRESHOLD = 0.15

# --- Performance Configuration ---

IN_DOCKER = os.environ.get('IN_DOCKER', '0') == '1'

if IN_DOCKER:
    # --- DOCKER SETTINGS (Linux/Fork) ---
    WORKERS_MAX = 20
    WORKERS_JACCARD = 10
else:
    # --- LOCAL WINDOWS SETTINGS (Spawn) ---
    WORKERS_JACCARD = min(6, multiprocessing.cpu_count())
    WORKERS_MAX = multiprocessing.cpu_count()