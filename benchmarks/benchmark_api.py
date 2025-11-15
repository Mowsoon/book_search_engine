import requests
import time
import statistics
import csv
import os

from scripts.config import DATA_DIR

# Configuration
API_URL = "http://127.0.0.1:8000/api"
ITERATIONS = 500
OUTPUT_FILE = os.path.join(DATA_DIR, "api_performance_stats.csv")

# Define test scenarios
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
        "url": f"{API_URL}/search?q=Victor Huga",
        "type": "Fuzzy"
    }
]


def run_test(scenario):
    """Runs a single scenario N times and collects timings."""
    print(f"--- Testing: {scenario['name']} ---")
    latencies = []

    # Warm-up request (to wake up caches/JIT)
    try:
        requests.get(scenario['url'])
    except:
        pass

    for i in range(ITERATIONS):
        try:
            start = time.time()
            resp = requests.get(scenario['url'], timeout=10)
            duration = (time.time() - start) * 1000  # Convert to ms

            if resp.status_code == 200:
                latencies.append(duration)
            else:
                print(f"Error {resp.status_code}")
        except Exception as e:
            print(f"Request failed: {e}")

    return latencies


def calculate_stats(latencies):
    """Computes statistical metrics."""
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
    print(f"🚀 Starting API Benchmark ({ITERATIONS} iterations per test)...")
    print("Ensure Django is running on port 8000!")

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

    # Save to CSV
    keys = all_results[0].keys()
    with open(OUTPUT_FILE, 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(all_results)

    print(f"\n✅ Benchmark complete. Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()