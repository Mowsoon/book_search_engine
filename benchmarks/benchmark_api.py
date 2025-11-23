import requests
import time
import statistics
import csv
import os
import sys

# Ajout du path pour config (juste pour récupérer DATA_DIR)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import config

# --- CONFIGURATION ---
# Dans le réseau Docker, le service s'appelle "online" (défini dans docker-compose)
# Le port interne est 8000.
DEFAULT_URL = "http://online:8000/api"

# On permet de surcharger via une variable d'env si besoin
API_URL = os.environ.get("API_URL", DEFAULT_URL)
ITERATIONS = 500
OUTPUT_FILE = os.path.join(config.PATHS["data"], "api_performance_stats.csv")

SCENARIOS = [
    {
        "name": "Simple: 'Frankenstein'",
        "url": f"{API_URL}/search?q=Frankenstein",
        "type": "Simple"
    },
    {
        "name": "Simple: 'Love'",
        "url": f"{API_URL}/search?q=Love",
        "type": "Simple"
    },
    {
        "name": "RegEx: 'Fran.*stein' (Prefix)",
        "url": f"{API_URL}/search/advanced?q=Fran.*stein",
        "type": "RegEx"
    },
    {
        "name": "RegEx: '.*hugo.*' (Wildcard)",
        "url": f"{API_URL}/search/advanced?q=.*hugo.*",
        "type": "RegEx"
    },
    {
        "name": "Recommendation: ID 84",
        "url": f"{API_URL}/book/84/suggestions",
        "type": "Graph"
    },
    {
        "name": "Fuzzy: 'Victor Huga'",
        "url": f"{API_URL}/search?q=Victor%20Huga",
        "type": "Fuzzy"
    }
]


def run_test(scenario):
    print(f"--- Testing: {scenario['name']} ---")
    latencies = []

    # Warm-up
    try:
        requests.get(scenario['url'])
    except:
        pass

    for i in range(ITERATIONS):
        try:
            start = time.time()
            resp = requests.get(scenario['url'], timeout=30)
            duration = (time.time() - start) * 1000

            if resp.status_code == 200:
                latencies.append(duration)
            else:
                if i == 0: print(f"⚠️ Status {resp.status_code}: {resp.text[:100]}")
        except Exception as e:
            if i == 0: print(f"⚠️ Connection failed: {e}")

    return latencies


def calculate_stats(latencies):
    if not latencies:
        return None
    return {
        "min": min(latencies),
        "max": max(latencies),
        "mean": statistics.mean(latencies),
        "median": statistics.median(latencies),
        "stdev": statistics.stdev(latencies) if len(latencies) > 1 else 0
    }


def main():
    print(f"🚀 STARTING INTERNAL DOCKER BENCHMARK")
    print(f"🎯 Target: {API_URL}")
    print(f"📊 Output: {OUTPUT_FILE}")

    all_results = []

    for scen in SCENARIOS:
        times = run_test(scen)
        stats = calculate_stats(times)

        if stats:
            print(f"   Mean: {stats['mean']:.2f}ms | Stdev: {stats['stdev']:.2f}ms")
            all_results.append({
                "Scenario": scen['name'],
                "Type": scen['type'],
                "Samples": len(times),
                "Min (ms)": round(stats['min'], 2),
                "Max (ms)": round(stats['max'], 2),
                "Mean (ms)": round(stats['mean'], 2),
                "Median (ms)": round(stats['median'], 2),
                "StDev (ms)": round(stats['stdev'], 2)
            })
        else:
            print("   ❌ Failed (0 successes)")

    if all_results:
        keys = all_results[0].keys()
        with open(OUTPUT_FILE, 'w', newline='') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(all_results)
        print(f"\n✅ Done.")


if __name__ == "__main__":
    main()