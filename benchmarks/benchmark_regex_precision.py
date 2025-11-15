import sys
import os
import time
import re
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

# Add parent dir to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import config

# --- Configuration ---
# We test RegEx of increasing complexity
REGEX_SCENARIOS = [
    r"frankenstein",  # Simple word (Baseline)
    r"fran.*stein",  # Prefix/Suffix (Standard case)
    r".*hugo.*",  # Full wildcard (Heavy for index)
    r"(white|black) cat",  # Alternation
]

# Limit books to process for the "Slow" method to avoid infinite runtime
MAX_DOCS_TO_PROCESS = 200


def get_es_client():
    """Returns an Elasticsearch client instance."""
    return Elasticsearch(config.ES_HOST)


def strategy_fast_index(regex, client):
    """
    Strategy A: Pure Index Search (Your API implementation).
    Elasticsearch filters documents. We do not read content.
    Score = 1.0 (binary match).
    """
    s = Search(using=client, index=config.ES_INDEX_NAME)
    # Standard RegEx query on inverted index
    s = s.query("regexp", content={"value": regex.lower(), "flags": "ALL"})

    start = time.time()
    # Fetch IDs only, no source content (optimized)
    response = s.source(False).params(track_total_hits=True).execute()
    duration = time.time() - start

    return duration, response.hits.total.value


def strategy_precise_compute(regex, client):
    """
    Strategy B: Simulated "True TF-IDF RegEx".
    1. Elastic filters candidates.
    2. We load raw text for each candidate.
    3. We count exact occurrences using Python re.findall.
    """
    pattern = re.compile(regex, re.IGNORECASE)

    # Step 1: Get candidates via Elastic
    s = Search(using=client, index=config.ES_INDEX_NAME)
    s = s.query("regexp", content={"value": regex.lower(), "flags": "ALL"})

    # Retrieve IDs to fetch files from disk
    # Limit results to keep benchmark feasible
    scan_response = s.source(False)[0:MAX_DOCS_TO_PROCESS].execute()
    candidate_ids = [hit.meta.id for hit in scan_response]

    start = time.time()

    processed_count = 0
    total_occurrences = 0

    # Step 2: The expensive loop (Simulating precise score calculation)
    for book_id in candidate_ids:
        file_path = os.path.join(config.BOOKS_DIR, f"{book_id}.txt")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Full text scan (CPU heavy)
                    matches = len(pattern.findall(content))
                    total_occurrences += matches
                    processed_count += 1
            except Exception:
                pass

    duration = time.time() - start
    return duration, processed_count


def main():
    print(f"🚀 STARTING PRECISION VS PERFORMANCE BENCHMARK")
    print(f"Comparison: Elastic Index vs Python Re-compute (Max {MAX_DOCS_TO_PROCESS} docs)")
    print("-" * 60)

    client = get_es_client()
    results = []

    for regex in REGEX_SCENARIOS:
        print(f"\n🧪 Testing RegEx: '{regex}'")

        # 1. Fast Test (Index)
        t_fast, count_fast = strategy_fast_index(regex, client)
        print(f"   [Fast] Index Search : {t_fast:.4f}s ({count_fast} docs found)")

        # 2. Precise Test (Re-compute)
        t_precise, count_processed = strategy_precise_compute(regex, client)

        # Projection: If we had to process ALL matching documents
        if count_fast > 0 and count_processed > 0:
            projected_time = t_precise * (count_fast / count_processed)
        else:
            projected_time = t_precise

        print(f"   [Slow] Full Compute : {t_precise:.4f}s (for {count_processed} docs)")
        print(f"   => Projected Total  : {projected_time:.4f}s")

        speedup = projected_time / t_fast if t_fast > 0 else 0

        results.append({
            "regex": regex,
            "docs_found": count_fast,
            "time_fast_index": round(t_fast, 5),
            "time_precise_compute_projected": round(projected_time, 5),
            "slowdown_factor": round(speedup, 1)
        })

    # Export CSV
    df = pd.DataFrame(results)
    output_file = os.path.join(config.DATA_DIR, "bench_precision_vs_perf.csv")
    df.to_csv(output_file, index=False)

    print("-" * 60)
    print(f"✅ Results saved to {output_file}")
    print("\n📊 SUMMARY:")
    print(df[['regex', 'time_fast_index', 'time_precise_compute_projected', 'slowdown_factor']].to_string())


if __name__ == "__main__":
    main()