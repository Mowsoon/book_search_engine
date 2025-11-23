# 📚 Gutenberg Search Engine

**Advanced Search Engine & Recommendation System for Project Gutenberg.**

This project is a high-performance web application capable of indexing thousands of books, performing full-text and Regex searches via **Elasticsearch**, and providing recommendations based on **Jaccard similarity** and **PageRank** centrality.

It relies on a **Hybrid Architecture**:

1.  **Offline System:** Heavy computations (downloading, indexing, graph building) are performed asynchronously to generate static assets (CSV indexes).
2.  **Online System:** A lightweight Django API loads these pre-computed assets into RAM for instant O(1) access, delegating text queries to an Elasticsearch cluster.

-----

## 🏗 Architecture Overview

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Search Engine** | Elasticsearch 8.11 | Full-text search, RegEx matching, TF-IDF scoring. |
| **API Server** | Django 5 + DRF | Orchestrates queries, merges TF-IDF with PageRank, serves JSON. |
| **Graph & Ranking**| NetworkX + Scipy | Computes Jaccard Similarity Graph and PageRank (Offline). |
| **Storage** | Local Filesystem | Books (`.txt`) and Indices (`.csv`) are stored on disk, mapped via Docker Volumes. |
| **Frontend** | HTML5 / JS | Lightweight Single Page Application (served by Django). |

-----

## 🚀 Prerequisities

To deploy this project, you only need **Docker** and **Docker Compose** installed on your machine.

  * [Install Docker Desktop](https://www.docker.com/products/docker-desktop/)

*No local Python installation is required as everything runs inside isolated containers.*

-----

## 🛠 Installation & Setup

### 1\. Clone the Repository

```bash
git clone https://github.com/your-username/book_search_engine.git
cd book_search_engine
```

### 2\. Build the Docker Images

This will build two distinct images: one for the web server (`online`) and one for the heavy computing worker (`offline`).

```bash
docker compose build
```

### 3\. Initialize Data (The "Offline" Phase)

Before starting the web server, we need to populate the database and compute the graphs. We use the `offline` container for this to avoid memory overhead on the server.

**Step A: Start Elasticsearch**

```bash
docker compose up -d elasticsearch
```

*Wait \~30 seconds for Elasticsearch to be ready.*

**Step B: Download Books**
This script fetches \>1670 books from Gutendex API (validation constraint: \>10k words/book).

```bash
docker compose run --rm offline python scripts/download_books.py
```

*(This might take a few minutes depending on your connection).*

**Step C: Index to Elasticsearch**
Pushes metadata and content to the search engine.

```bash
docker compose run --rm offline python scripts/index_to_elasticsearch.py
```

**Step D: Build Graphs & PageRank (CPU Intensive)**
Computes Jaccard similarity and PageRank centrality.
*Note: We stop Elasticsearch temporarily to free up RAM for this heavy calculation.*

```bash
docker compose stop elasticsearch
docker compose run --rm offline python scripts/build_graphs.py
docker compose start elasticsearch
```

-----

## 🏃 Running the Application

Once initialization is complete, start the **Online System** (API + Database).

```bash
docker compose up -d online
```

The application is now accessible at:
👉 **http://localhost:8000/**

### Stop the Application

```bash
docker compose down
```

*(Note: Data is persisted in the `data/` folder and docker volumes. You don't need to re-run initialization after a restart).*

-----

## 🧪 Features & Usage

### 1\. Web Interface

Go to `http://localhost:8000/`.

  * **Simple Search:** Type a keyword (e.g., "Frankenstein"). Ranking relies on TF-IDF + PageRank.
  * **Advanced Search:** Type a Python-style RegEx (e.g., `.*hugo.*` or `(love|hate)`).
  * **Recommendation:** Click on "Show Similar Books" on any card to see neighbors from the Jaccard Graph.
  * **Read:** Click "Read" to open the book content instantly.

### 2\. API Endpoints

You can consume the API directly:

  * **Search:** `GET /api/search?q=keyword`
  * **RegEx:** `GET /api/search/advanced?q=regex`
  * **Suggestions:** `GET /api/book/<id>/suggestions`
  * **Content:** `GET /api/book/<id>/content`

-----

## 📊 Performance & Benchmarks

The project includes a comprehensive benchmark suite located in `benchmarks/`.

To reproduce the performance tests described in the report:

```bash
# 1. Test RegEx Performance (Index vs Full Scan)
docker compose run --rm offline python benchmarks/benchmark_regex_precision.py

# 2. Test API Latency (Need 'online' container running)
# (Run this from your local machine if python is installed, or inside the online container)
python benchmarks/benchmark_api.py
```

**Key Performance Metrics:**

  * **Graph Computation:** \~9 minutes for 1.4 million comparisons (optimized via Multiprocessing).
  * **Search Latency:** \~50ms for indexed search vs \~20s for linear scan.
  * **RAM Usage:** Optimized to run on standard laptops by offloading graph data to static CSVs loaded in memory.

-----

## 📂 Project Structure

```text
.
├── back_end/           # Django Application (Core Logic)
│   ├── gutenberg_api/  # Search & Ranking Logic (Views)
│   └── core/           # Project Settings
├── front_end/          # Client-side application (HTML/JS)
├── scripts/            # Offline ETL Scripts (Download, Index, Graph)
├── benchmarks/         # Performance testing scripts
├── data/               # Shared volume for Books (.txt) and Indexes (.csv)
├── docker-compose.yaml # Infrastructure orchestration
├── Dockerfile.online   # Lightweight image for the API
└── Dockerfile.offline  # Heavy image with Scipy/NetworkX for calculations
```