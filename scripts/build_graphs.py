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

def get_stopwords():
    """Loads combined French and English stopwords."""
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)

    en_stops = set(stopwords.words('english'))
    fr_stops = set(stopwords.words('french'))
    return en_stops.union(fr_stops)


def clean_text(text, stop_words):
    """Tokenizes and cleans text."""
    if not text:
        return set()
    text = re.sub(r'[^\w\s]', '', text.lower())
    return {
        word for word in text.split()
        if word not in stop_words and len(word) > 2
    }


def load_books():
    """Loads and cleans all books."""
    books = {}
    stop_words = get_stopwords()

    if not os.path.exists(config.BOOKS_DIR):
        print(f"[ERROR] No books found in {config.BOOKS_DIR}")
        return {}

    file_list = [f for f in os.listdir(config.BOOKS_DIR) if f.endswith(".txt")]
    print(f"Loading and cleaning {len(file_list)} books...")

    for filename in file_list:
        book_id = filename.replace(".txt", "")
        path = os.path.join(config.BOOKS_DIR, filename)
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                books[book_id] = clean_text(content, stop_words)
        except IOError:
            pass
    return books


def compute_jaccard(set_a, set_b):
    """Computes Jaccard similarity."""
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    return intersection / union if union > 0 else 0.0

# --- WORKER FUNCTIONS ---

def worker_init(books_dict, books_ids):
    """Initialize worker for Jaccard calculation."""
    global SHARED_BOOKS, SHARED_IDS
    SHARED_BOOKS = books_dict
    SHARED_IDS = books_ids

def worker_init_graph(graph):
    """Initialize worker for Closeness calculation."""
    global SHARED_GRAPH
    SHARED_GRAPH = graph


def worker_compare_row(i):
    """Worker function: Compares book[i] against all books[j] > i."""
    local_edges = []
    id_a = SHARED_IDS[i]
    set_a = SHARED_BOOKS[id_a]
    n = len(SHARED_IDS)

    for j in range(i + 1, n):
        id_b = SHARED_IDS[j]
        set_b = SHARED_BOOKS[id_b]

        score = compute_jaccard(set_a, set_b)
        if score > config.JACCARD_THRESHOLD:
            local_edges.append((id_a, id_b, score))

    return local_edges

def worker_closeness(nodes_subset):
    """Worker: Calculates closeness for a subset of nodes."""
    res = {}
    for node in nodes_subset:
        # distance='weight' interprets weight as distance (cost)
        # But Jaccard is a similarity (higher is better).
        # NetworkX handles this usually by 1/weight or we assume the edges are pre-processed.
        # For standard closeness on weighted graphs, 'distance' attribute is edge weight.
        res[node] = nx.closeness_centrality(SHARED_GRAPH, u=node, distance='weight')
    return res


# --- ORCHESTRATION ---

def build_edges_parallel(books):
    """Orchestrates parallel graph construction."""
    book_ids = list(books.keys())
    n = len(book_ids)
    # --- OPTIMIZATION: Use limited workers for RAM-heavy task ---
    cores = config.WORKERS_JACCARD

    print(f"Computing similarities for {n} books on {cores} CPU cores...")
    start = time.time()

    with Pool(processes=cores, initializer=worker_init, initargs=(books, book_ids)) as pool:
        results = pool.map(worker_compare_row, range(n), chunksize=10)

    edges = [edge for sublist in results for edge in sublist]

    elapsed = time.time() - start
    print(f"Graph computation finished in {elapsed:.2f}s. Found {len(edges)} edges.")
    return edges


def compute_centrality_parallel(edges):
    """Builds graph and computes metrics in parallel."""
    graph = nx.Graph()

    # Invert weights for Closeness (Distance = 1 - Similarity)
    for u, v, w in edges:
        dist = 1.0 - w if w < 1.0 else 0.001
        graph.add_edge(u, v, weight=dist)

    print(f"Graph built: {graph.number_of_nodes()} nodes.")
    pr_graph = nx.Graph()
    pr_graph.add_weighted_edges_from(edges)

    print("Calculating PageRank...")
    pagerank = nx.pagerank(pr_graph, weight='weight')

    # 2. Closeness (Parallelized with MAX cores)
    # --- OPTIMIZATION: Use ALL cores for CPU-heavy/RAM-light task ---
    cores = config.WORKERS_CLOSENESS

    print(f"Calculating Closeness on {cores} cores...")
    nodes_list = list(graph.nodes())

    if nodes_list:
        # Split nodes into chunks for workers
        # Handle edge case if cores > nodes
        num_chunks = min(len(nodes_list), cores)
        chunks = np.array_split(nodes_list, num_chunks)

        start_c = time.time()
        with Pool(processes=cores, initializer=worker_init_graph, initargs=(graph,)) as pool:
            results = pool.map(worker_closeness, chunks)

        # Merge results
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
    """Saves results to CSV."""
    df_ranks.sort_values("pagerank", ascending=False, inplace=True)
    df_ranks.to_csv(config.RANK_FILE, index=False)

    df_edges = pd.DataFrame(edges, columns=["source", "target", "weight"])
    df_edges.to_csv(config.GRAPH_FILE, index=False)
    print(f"Saved data to {config.DATA_DIR}")


if __name__ == "__main__":
    # 1. Load
    book_data = load_books()

    if book_data:
        # 2. Build Graph (Parallel)
        graph_edges = build_edges_parallel(book_data)

        if graph_edges:
            # 3. Metrics (Sequential) & Save
            ranks_df = compute_centrality_parallel(graph_edges)
            save_data(ranks_df, graph_edges)
        else:
            print("No edges found.")
