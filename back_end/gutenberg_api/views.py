import os

from django.apps import apps
from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

# ES Client configuration
ES_URL = os.environ.get("ES_HOST", "http://localhost:9200")
ES_CLIENT = Elasticsearch(ES_URL)
ES_INDEX = "gutenberg_books"


class BaseSearchView(APIView):
    """Base class for shared ranking logic."""

    def get_app_data(self):
        """Retrieve in-memory data from AppConfig."""
        return apps.get_app_config('gutenberg_api')

    def calculate_ranking(self, es_results):
        """
        Merges ES score (TF-IDF) with PageRank (Centrality).
        Formula: Score = (Norm_ES * 0.7) + (Norm_PR * 0.3)
        """
        book_ranks = self.get_app_data().book_ranks
        ranked_results = []

        # Find max ES score for normalization
        max_es = max((hit.meta.score for hit in es_results), default=1.0)
        if max_es == 0:
            max_es = 1.0

        for hit in es_results:
            book_id = hit.gutenberg_id
            es_score = hit.meta.score

            # Get offline scores (default to 0 if missing)
            stats = book_ranks.get(book_id, {'pagerank': 0, 'closeness': 0})
            pagerank = stats['pagerank']

            # Normalize and combine
            # Multiply PR by 50 to bring it to a scale comparable to TF-IDF
            norm_es = es_score / max_es
            norm_pr = pagerank * 50

            final_score = (norm_es * 0.7) + (norm_pr * 0.3)

            ranked_results.append({
                'id': book_id,
                'title': hit.title,
                'author': hit.author,
                'image_url': hit.image_url,
                'score': round(final_score, 4),
                'details': {
                    'tf_idf': round(es_score, 2),
                    'pagerank': f"{pagerank:.6f}"
                }
            })

        # Sort by final score descending
        ranked_results.sort(key=lambda x: x['score'], reverse=True)
        return ranked_results


class SimpleSearchView(BaseSearchView):
    """
    GET /api/search?q=keyword
    Full-text search using ElasticSearch multi_match.
    """

    def get(self, request):
        query = request.query_params.get('q', '').strip()
        if not query:
            return Response({"error": "Missing 'q' parameter"}, status=400)

        s = Search(using=ES_CLIENT, index=ES_INDEX)
        # Search in title (boosted), author, and content
        s = s.query("multi_match", query=query,
                    fields=['title^3', 'author^2', 'content'])

        response = s[0:50].execute()
        final_results = self.calculate_ranking(response)

        return Response({
            "count": len(final_results),
            "results": final_results[:20]
        })


class AdvancedSearchView(BaseSearchView):
    """
    GET /api/search/advanced?q=RegEx
    RegEx search on book content.
    """

    def get(self, request):
        regex = request.query_params.get('q', '').strip()
        if not regex:
            return Response({"error": "Missing 'q' parameter"}, status=400)

        regex = regex.lower()

        s = Search(using=ES_CLIENT, index=ES_INDEX)
        s = s.query("regexp", content={"value": regex, "flags": "ALL"})

        try:
            response = s[0:50].execute()
        except Exception as e:
            return Response({"error": f"Search Error: {str(e)}"}, status=400)

        final_results = self.calculate_ranking(response)

        return Response({
            "count": len(final_results),
            "results": final_results
        })


class SuggestionView(APIView):
    """
    GET /api/book/<id>/suggestions
    Returns neighbors from the Jaccard graph.
    """

    def get(self, request, book_id):
        graph = apps.get_app_config('gutenberg_api').book_graph

        # 1. Get neighbors IDs from memory
        neighbor_ids = graph.get(book_id, [])

        if not neighbor_ids:
            return Response({"results": []})

        # 2. Fetch details from ElasticSearch (Multi-Get)
        s = Search(using=ES_CLIENT, index=ES_INDEX)
        s = s.filter("terms", gutenberg_id=neighbor_ids)
        response = s[0:10].execute()

        suggestions = [{
            'id': hit.gutenberg_id,
            'title': hit.title,
            'author': hit.author,
            'image_url': hit.image_url
        } for hit in response]

        return Response({"results": suggestions})


class BookContentView(APIView):
    """
    GET /api/book/<id>/content
    Reads the local .txt file and returns its content.
    """

    def get(self, request, book_id):
        # Construct absolute path to the book file
        file_path = os.path.join(settings.DATA_DIR, 'books', f"{book_id}.txt")

        if not os.path.exists(file_path):
            return Response({"error": "Book text not found locally."}, status=404)

        try:
            # Read the file
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Return as JSON (safer than raw text for frontend handling)
            return Response({"id": book_id, "content": content})

        except Exception as e:
            return Response({"error": f"Error reading file: {str(e)}"}, status=500)