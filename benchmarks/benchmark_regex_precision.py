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
REGEX_SCENARIOS = [
    r"frankenstein",  # Simple
    r"fran.*stein",  # Prefix/Suffix
    r".*hugo.*",  # Wildcard (Heavy)
    r"(love|hate)",  # Alternation (Common words)
]

# Steps for the growth curve test (Number of docs to scan manually)
GROWTH_STEPS = [10, 50, 100, 200, 300]
# The regex used for the growth test (must have many matches)
GROWTH_REGEX = r".*hugo.*"

# We use a fixed small number for the general comparison table
MAX_DOCS_FOR_COMPARE = 50


def get_es_client():
    """Returns an Elasticsearch client instance."""
    return Elasticsearch(config.ELASTIC["host"])


def strategy_fast_index(regex, client):
    """
    Strategy A: Pure Index Search.
    Measures the time for Elasticsearch to find matching documents.
    """
    s = Search(using=client, index=config.ELASTIC["index_name"])
    s = s.query("regexp", content={"value": regex.lower(), "flags": "ALL"})

    # Fix for DeprecationWarning: Use extra() for body params like track_total_hits
    s = s.extra(track_total_hits=True)

    start = time.time()
    # Fetch IDs only, no source content
    response = s.source(False).execute()
    duration = time.time() - start

    return duration, response.hits.total.value


def strategy_precise_compute(regex, client, limit_docs):
    """
    Strategy B: Simulated "True TF-IDF RegEx".
    Loads 'limit_docs' files and scans them with Python re.
    """
    pattern = re.compile(regex, re.IGNORECASE)

    # 1. Get candidate IDs from Elastic
    s = Search(using=client, index=config.ELASTIC["index_name"])
    s = s.query("regexp", content={"value": regex.lower(), "flags": "ALL"})

    # Fetch enough IDs to cover the test limit
    # We scan up to 'limit_docs'
    scan_response = s.source(False)[0:limit_docs].execute()
    candidate_ids = [hit.meta.id for hit in scan_response]

    # If we don't have enough docs to test the limit, we stop early
    if len(candidate_ids) < limit_docs:
        # Optional: warn user, but for benchmark we just process what we have
        pass

    start = time.time()

    processed_count = 0
    total_occurrences = 0

    # 2. The expensive loop
    for book_id in candidate_ids:
        file_path = os.path.join(config.PATHS["books"], f"{book_id}.txt")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    matches = len(pattern.findall(content))
                    total_occurrences += matches
                    processed_count += 1
            except Exception:
                pass

    duration = time.time() - start
    return duration, processed_count


def main():
    client = get_es_client()



    # --- PART 1: SCENARIO COMPARISON ---
    print(f"\n🚀 PART 1: SCENARIO COMPARISON (Fixed limit: {MAX_DOCS_FOR_COMPARE} docs)")
    print("-" * 60)

    results_scenarios = []



    for regex in REGEX_SCENARIOS:
        print(f"🧪 Testing: '{regex}'")

        t_fast, count_fast = strategy_fast_index(regex, client)
        t_precise, count_proc = strategy_precise_compute(regex, client, MAX_DOCS_FOR_COMPARE)

        # Project time if we had to process ALL matching docs
        if count_fast > 0 and count_proc > 0:
            projected = t_precise * (count_fast / count_proc)
        else:
            projected = 0

        speedup = projected / t_fast if t_fast > 0 else 0

        results_scenarios.append({
            "regex": regex,
            "docs_found": count_fast,
            "time_index": round(t_fast, 4),
            "time_compute_projected": round(projected, 4),
            "slowdown_factor": round(speedup, 1)
        })

    df_scenarios = pd.DataFrame(results_scenarios)
    print("\n📊 SCENARIO SUMMARY:")
    print(df_scenarios.to_string())
    df_scenarios.to_csv(os.path.join(config.PATHS["data"], "bench_scenarios.csv"), index=False)

    # --- PART 2: GROWTH CURVE ---
    print(f"\n\n🚀 PART 2: GROWTH CURVE (RegEx: '{GROWTH_REGEX}')")
    print("Demonstrating O(N) complexity of manual scanning vs O(1) index.")
    print("-" * 60)

    results_growth = []

    for n_docs in GROWTH_STEPS:
        print(f"📈 Benchmarking for N = {n_docs} docs...")

        # 1. Elastic (Index) - Time is roughly constant
        t_fast, _ = strategy_fast_index(GROWTH_REGEX, client)

        # 2. Python (Compute) - Time should grow linearly
        t_precise, count_proc = strategy_precise_compute(GROWTH_REGEX, client, n_docs)

        if count_proc < n_docs:
            print(f"   [WARN] Only found {count_proc} docs matching regex. Growth curve might be capped.")

        results_growth.append({
            "n_docs": count_proc,
            "time_elastic_index": t_fast,
            "time_python_compute": t_precise
        })

    df_growth = pd.DataFrame(results_growth)
    output_growth = os.path.join(config.PATHS["data"], "bench_growth_curve.csv")
    df_growth.to_csv(output_growth, index=False)

    print(f"\n✅ Growth data saved to {output_growth}")
    print("Use this CSV to plot: X=n_docs, Y1=time_elastic, Y2=time_python")
    print(df_growth.to_string())


if __name__ == "__main__":
    main()