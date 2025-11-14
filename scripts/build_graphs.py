# scripts/build_graphs.py
import os
import re
import time
import pandas as pd
import networkx as nx
import nltk
from nltk.corpus import stopwords


try:
    import config
except ImportError:
    from scripts import config


def get_stopwords():
    """
    Loads combined French and English stopwords.
    """

    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)

    en_stops = set(stopwords.words('english'))
    fr_stops = set(stopwords.words('french'))
    return en_stops.union(fr_stops)


def clean_text(text, stop_words):
    """
        Tokenizes and cleans text.
        Returns a set of unique significant words.
    """
    if not text:
        return set()

    # Lowercase and remove punctuation using regex
    text = re.sub(r'[^\w\s]', '', text.lower())

    # Split and filter
    return{
        word for word in text.split()
        if word not in stop_words and len(word) > 2
    }


def load_books():
    """Loads and cleans all books.
    Returns: dict {book_id: set_of_words}
    """
    books = {}
    stop_words = get_stopwords()

    if not os.path.exists(config.BOOKS_DIR):
        print(f"[ERROR] No books found in {config.BOOKS_DIR}")
        return {}

    file_list = [f for f in os.listdir(config.BOOKS_DIR) if f.endswith(".txt")]
    print(f"Loading and cleaning {len(file_list)} books...")

    for i, filename in enumerate(file_list):
        book_id = filename.replace(".txt", "")
        path = os.path.join(config.BOOKS_DIR, filename)

        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                books[book_id] = clean_text(content, stop_words)
        except IOError:
            pass

        if (i + 1) % 200 == 0:
            print(f"  Processed {i + 1} books.")

    return books

def compute_jaccard(set_a, set_b):
    """Computes Jaccard similarity between two sets."""
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    return intersection / union if union > 0 else 0.0

def build_edges(books):
    """
        Compares all pairs of books.
        Returns: list of (source, target, weight)
    """
    edges = []
    ids = list(books.keys())
    n = len(ids)

    print(f"Computing similarity for {n} books (~{n * n // 2} pairs)...")
    start = time.time()

    for i in range(n):
        for j in range(i + 1, n):
            id_a, id_b = ids[i], ids[j]
            sim = compute_jaccard(books[id_a], books[id_b])

            if sim >= config.JACCARD_THRESHOLD:
                edges.append((id_a, id_b, sim))
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start
            print(f"  Processed {i + 1} books in {elapsed:.0f}s")

    print(f"Found {len(edges)} edges above threshold {config.JACCARD_THRESHOLD}.")
    return edges


def compute_centrality(edges):
    """
        Builds graph and computes PageRank and Closeness.
        Returns: DataFrame
    """
    graph = nx.Graph()
    graph.add_weighted_edges_from(edges)

    print(f"Graph built: {graph.number_of_nodes()} nodes.")
    print("Calculating PageRank...")
    pagerank = nx.pagerank(graph, weight="weight")

    print("Calculating Closeness Centrality...")
    closeness = nx.closeness_centrality(graph, distance="weight")

    data = []
    for node in graph.nodes():
        data.append({
            "id": node,
            "pagerank": pagerank.get(node, 0),
            "closeness": closeness.get(node, 0)
        })

    return pd.DataFrame(data)

def save_data(df_ranks, edges):
    """
    Saves results to CSV.
    """
    # Save Ranks
    (df_ranks.sort_values("pagerank", ascending=False, inplace=True)
            .to_csv(config.RANK_FILE, index=False))
    print(f"Ranks saved to {config.RANK_FILE}")

    # Save Edges
    df_edges = (pd.DataFrame(edges, columns=["source", "target", "weight"])
                .to_csv(config.GRAPH_FILE, index=False))

    print(f"Graph edges saved to {config.GRAPH_FILE}")


if __name__ == "__main__":
    # 1. Load
    book_data = load_books()

    if book_data:
        # 2. Build Graph
        graph_edges = build_edges(book_data)

        if graph_edges:
            # 3. Metrics
            ranks_df = compute_centrality(graph_edges)
            # 4. Save
            save_data(ranks_df, graph_edges)
        else:
            print("No edges found. Try lowering JACCARD_THRESHOLD in config.")
