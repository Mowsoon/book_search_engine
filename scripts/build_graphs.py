import os
import re
import time
import pandas as pd
import networkx as nx
import nltk
from nltk.corpus import stopwords
from multiprocessing import Pool
import numpy as np

try:
    import config
except ImportError:
    from scripts import config

# --- Global variables for Multiprocessing workers ---
SHARED_BOOKS = {}
SHARED_IDS = []
SHARED_GRAPH = None
STOP_WORDS = set()

def init_worker_loader():
    """Initializes the worker's shared variables."""
    global STOP_WORDS
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)

    en_stops = set(stopwords.words('english'))
    fr_stops = set(stopwords.words('french'))
    STOP_WORDS = en_stops.union(fr_stops)

def process_single_book_file(filename):
    """Worker function to load and clean one book file."""
    book_id = filename.replace(".txt", "")
    path = os.path.join(config.PATHS["books"], filename)

    try:
        with open(path, "r", encoding="utf-8") as f:
            text= f.read()

        if not text:
            return None

        text = re.sub(r'[^\w\s]', '', text.lower())
        unique_words = {
            word for word in text.split()
            if word not in STOP_WORDS and len(word) > 2
        }

        return book_id, unique_words
    except IOError:
        return None


def load_books_parallel():
    """Loads all book files in parallel."""
    books_dir = config.PATHS["books"]
    if not os.path.exists(books_dir):
        print(f"[ERROR] No books found in {books_dir}")
        return {}
    file_list = [f for f in os.listdir(books_dir) if f.endswith(".txt")]

    workers = config.WORKERS.cpu_intensive
    print(f"Loading and cleaning {len(file_list)} books on {workers} cores...")

    start = time.time()
    with Pool(processes=workers, initializer=init_worker_loader) as pool:
        results = pool.map(process_single_book_file, file_list, chunksize=20)

    books = {r[0]: r[1] for r in results if r is not None}

    print(f"Loading finished in {time.time() - start:.2f}s.")
    return books

def compute_jaccard(set_a, set_b):
    """Computes Jaccard similarity."""
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    return intersection / union if union > 0 else 0.0

# --- WORKER FUNCTIONS (GRAPH) ---


def worker_init_books(books_dict, books_ids):
    global SHARED_BOOKS, SHARED_IDS
    SHARED_BOOKS = books_dict
    SHARED_IDS = books_ids

def worker_init_graph(graph):
    global SHARED_GRAPH
    SHARED_GRAPH = graph


def worker_compare_row(i):
    local_edges = []
    id_a = SHARED_IDS[i]
    set_a = SHARED_BOOKS[id_a]
    n = len(SHARED_IDS)

    threshold = config.CONSTRAINTS["jaccard_threshold"]
    for j in range(i + 1, n):
        id_b = SHARED_IDS[j]
        set_b = SHARED_BOOKS[id_b]

        score = compute_jaccard(set_a, set_b)
        if score > threshold:
            local_edges.append((id_a, id_b, score))
    return local_edges

def worker_closeness(nodes_subset):
    res = {}
    for node in nodes_subset:
        res[node] = nx.closeness_centrality(SHARED_GRAPH, u=node, distance='weight')
    return res

# --- ORCHESTRATION ---

def build_edges_parallel(books):
    book_ids = list(books.keys())
    n = len(book_ids)
    cores = config.WORKERS.ram_intensive

    print(f"Computing similarities for {n} books on {cores} CPU cores (RAM Safe)...")
    start = time.time()

    with Pool(processes=cores, initializer=worker_init_books, initargs=(books, book_ids)) as pool:
        results = pool.map(worker_compare_row, range(n), chunksize=10)

    edges = [edge for sublist in results for edge in sublist]

    elapsed = time.time() - start
    print(f"Jaccard computation finished in {elapsed:.2f}s. Found {len(edges)} edges.")
    return edges


def compute_centrality_parallel(edges):
    graph = nx.Graph()
    for u, v, w in edges:
        dist = 1.0 - w if w < 1.0 else 0.001
        graph.add_edge(u, v, weight=dist)

    print(f"Graph built: {graph.number_of_nodes()} nodes.")

    # PageRank (Sequential is fast enough)
    pr_graph = nx.Graph()
    pr_graph.add_weighted_edges_from(edges)
    print("Calculating PageRank...")
    pagerank = nx.pagerank(pr_graph, weight='weight')

    # Closeness (Parallel - Max Cores)
    cores = config.WORKERS.cpu_intensive

    print(f"Calculating Closeness on {cores} cores (CPU Intensive)...")
    nodes_list = list(graph.nodes())

    if nodes_list:
        num_chunks = min(len(nodes_list), cores)
        chunks = np.array_split(nodes_list, num_chunks)

        start_c = time.time()
        with Pool(processes=cores, initializer=worker_init_graph, initargs=(graph,)) as pool:
            results = pool.map(worker_closeness, chunks)

        closeness = {}
        for res in results:
            closeness.update(res)
        print(f"Closeness calculation took {time.time() - start_c:.2f}s.")
    else:
        closeness = {}

    return pd.DataFrame([
        {
            "id": node,
            "pagerank": pagerank.get(node, 0),
            "closeness": closeness.get(node, 0)
        }
        for node in nodes_list
    ])


def save_data(df_ranks, edges):
    if df_ranks.empty:
        print("[WARN] No ranking data to save.")
        return

    rank_file = config.PATHS["ranks_csv"]
    graph_file = config.PATHS["graph_csv"]
    data_dir = config.PATHS["data"]

    df_ranks.sort_values("pagerank", ascending=False, inplace=True)
    df_ranks.to_csv(rank_file, index=False)

    df_edges = pd.DataFrame(edges, columns=["source", "target", "weight"])
    df_edges.to_csv(graph_file, index=False)
    print(f"Saved data to {data_dir}")

if __name__ == "__main__":
    print("Starting Graph Build Script...")

    # 1. Parallel Load
    book_data = load_books_parallel()

    if book_data:
        # 2. Parallel Graph (Reduced cores for RAM)
        graph_edges = build_edges_parallel(book_data)

        if graph_edges:
            # 3. Parallel Metrics (Max cores for CPU)
            ranks_df = compute_centrality_parallel(graph_edges)
            save_data(ranks_df, graph_edges)
        else:
            print("No edges found.")

    print("Script finished successfully.")