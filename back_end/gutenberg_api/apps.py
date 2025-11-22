import os
import pandas as pd
from django.apps import AppConfig
from django.conf import settings


class GutenbergApiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'gutenberg_api'

    # Global in-memory storage for indexes
    # {book_id: {'pagerank': float, 'closeness': float}}
    book_ranks = {}
    # {book_id: [neighbor_id_1, neighbor_id_2...]}
    book_graph = {}

    def ready(self):
        """Loads CSV data into memory when Django starts."""
        # Prevent running twice with auto-reloader
        if os.environ.get('RUN_MAIN', None) != 'true':
            return
        # 1. Load Ranks
        rank_path = os.path.join(settings.DATA_DIR, 'book_ranks.csv')
        if os.path.exists(rank_path):
            try:
                df = pd.read_csv(rank_path)
                # Dict for O(1) access: id -> row_dict
                self.book_ranks = df.set_index('id').to_dict('index')
                print(f"Loaded {len(self.book_ranks)} ranks.")
            except Exception as e:
                print(f"Error loading ranks: {e}")
        else:
            print(f"Warning: {rank_path} missing.")

        # 2. Load Graph (Adjacency List)
        graph_path = os.path.join(settings.DATA_DIR, 'book_graph.csv')
        if os.path.exists(graph_path):
            try:
                df = pd.read_csv(graph_path)
                # Group by source to get neighbors
                grouped = df.groupby('source')['target'].apply(list)
                self.book_graph = grouped.to_dict()
                print(f"Loaded graph for {len(self.book_graph)} books.")
            except Exception as e:
                print(f"Error loading graph: {e}")
        else:
            print(f"Warning: {graph_path} missing.")