import os
import multiprocessing

# --- 1. ENVIRONMENT CONTEXT ---
# We detect the environment once to drive logic later
IN_DOCKER = os.environ.get('IN_DOCKER', '0') == '1'
SYSTEM_CORES = multiprocessing.cpu_count()

# --- 2. PATHS & FILES ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

PATHS = {
    "data": os.path.join(project_root, "data"),
    "books": os.path.join(project_root, "data", "books"),
    "metadata": os.path.join(project_root, "data", "metadata.json"),
    "graph_csv": os.path.join(project_root, "data", "book_graph.csv"),
    "ranks_csv": os.path.join(project_root, "data", "book_ranks.csv"),
}

# --- 3. ELASTICSEARCH CONFIGURATION ---
# Grouping ES settings makes the client initialization cleaner
ELASTIC = {
    "host": os.environ.get('ES_HOST', 'http://localhost:9200'),
    "index_name": 'gutenberg_books',
    "timeout": 30,
    "bulk_chunk_size": 50
}

# --- 4. PROJECT CONSTRAINTS ---
CONSTRAINTS = {
    "target_books": 1670,
    "min_words_per_book": 10000,
    "jaccard_threshold": 0.15
}


# --- 5. PERFORMANCE STRATEGY ---
class ResourceAllocator:
    """
    Centralizes the logic for resource allocation.
    Developers just ask for 'heavy_ram' or 'heavy_cpu' workers,
    without worrying about Docker or Windows specifics.
    """

    @property
    def ram_intensive(self):
        """
        For tasks with high memory duplication (e.g., Jaccard Graph).
        """
        if IN_DOCKER:
            return 12
        return min(6, SYSTEM_CORES)  # Safety limit for Windows spawn

    @property
    def cpu_intensive(self):
        """
        For tasks with low memory footprint (e.g., Closeness, PageRank).
        """
        if IN_DOCKER:
            return 20  # Docker CPU limit
        return SYSTEM_CORES

    @property
    def io_intensive(self):
        """
        For Network/Disk tasks (e.g., Elastic Indexing, Downloading).
        """
        return 20 if IN_DOCKER else 8


WORKERS = ResourceAllocator()