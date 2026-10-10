# 🎬 Kino — Big Data Movie Recommendation System
### *Project Presentation*

---

## 📌 Slide 1 — Title Slide

**Project Title:** Kino — Big Data Movie Recommendation System

**Tagline:** *A Netflix-inspired movie recommendation engine powered by PySpark, FastAPI, HBase, and the MovieLens 20M dataset.*

**Team Members:** [Your Team Members Here]

**Course / Subject:** [Your Course Name Here]

**Date:** [Presentation Date]

---

## 📌 Slide 2 — Problem Statement

### The Problem

- With **thousands of movies** available today, users face **decision fatigue** — they don't know what to watch.
- Traditional search requires knowing exact titles, which fails when users only know how they *feel* ("I want something scary") rather than *what* they want.
- Most basic recommendation systems use small datasets and lack the scalability to handle millions of real-world user interactions.

### Our Solution

Build a **full-stack, Big Data-powered movie recommendation engine** that:
1. Processes **20 million+ real user ratings** using distributed computing (PySpark).
2. Supports **mood-based natural language search** (NLP keyword mapping).
3. Uses **HBase (NoSQL)** as a scalable database serving layer.
4. Delivers results through a stunning, **Netflix-inspired dark-theme UI**.

---

## 📌 Slide 3 — Objectives

1. **Process Large-Scale Data** — Aggregate 20M+ ratings from the MovieLens dataset using Apache Spark (PySpark) to compute per-movie statistics.
2. **Implement Weighted Ratings** — Apply the IMDB Weighted Rating formula to eliminate bias from movies with very few votes.
3. **Mood-Based Search** — Build an NLP-driven mood-to-genre mapping engine so users can search by emotion/description.
4. **NoSQL Integration** — Design and implement an HBase column-family database schema for scalable movie data storage.
5. **Real-Time Data Enrichment** — Fetch movie posters, backdrops, and synopses from TMDb API using concurrent multi-threaded requests.
6. **Premium Frontend** — Build a responsive, Netflix-themed Single Page Application (SPA) with carousels, modals, and interactive search.

---

## 📌 Slide 4 — Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Big Data Processing** | Apache Spark (PySpark) | Distributed processing of 20M+ ratings |
| **Backend API** | FastAPI (Python) | RESTful API server with async support |
| **NoSQL Database** | Apache HBase | Column-family database for movie data |
| **Database Client** | happybase (Thrift) | Python → HBase communication |
| **Data Enrichment** | TMDb API | Movie posters, backdrops, synopses |
| **Concurrency** | ThreadPoolExecutor | 30 concurrent threads for API fetching |
| **Data Manipulation** | Pandas | In-memory DataFrames for serving |
| **Frontend** | HTML5, CSS3, JavaScript | Netflix-themed responsive SPA |
| **Web Server** | Uvicorn (ASGI) | Production-grade async server |
| **Dataset** | MovieLens 20M | 20,000,000 ratings across 27,000 movies |

---

## 📌 Slide 5 — System Architecture & Data Flow

```
┌──────────────────────────────┐
│   Raw MovieLens CSVs         │
│   (20M ratings, ~690 MB)     │
└──────────────┬───────────────┘
               │
               ▼  PySpark (local[*])
┌──────────────────────────────┐
│   Spark Aggregation Engine   │
│   - groupBy MovieID          │
│   - avg(Rating)              │
│   - count(Rating)            │
│   - IMDB Weighted Rating     │
│   - Filter: ratings ≥ 50     │
└──────────────┬───────────────┘
               │
               ▼  Pandas Export
┌──────────────────────────────┐
│   movies_with_ratings.csv    │
└──────────────┬───────────────┘
               │
               ▼  TMDb API (30 threads)
┌──────────────────────────────┐
│   fetch_posters.py           │
│   - MovieLens → TMDb ID map  │
│   - Poster, backdrop, synop. │
│   - JSON cache for re-runs   │
└──────────────┬───────────────┘
               │
               ▼  Export
┌──────────────────────────────┐
│   movies_with_posters.csv    │
└──────────────┬───────────────┘
               │
        ┌──────┴──────┐
        ▼             ▼
   ┌─────────┐  ┌───────────────┐
   │  HBase  │  │  CSV Fallback │
   │ (NoSQL) │  │    (Backup)   │
   └────┬────┘  └───────┬───────┘
        └──────┬────────┘
               ▼
┌──────────────────────────────┐
│   FastAPI Backend (app.py)   │
│   REST JSON API endpoints    │
└──────────────┬───────────────┘
               │
               ▼  JSON
┌──────────────────────────────┐
│   Netflix-themed SPA (UI)    │
│   Carousels, Modals, Search  │
└──────────────────────────────┘
```

---

## 📌 Slide 6 — Big Data Processing (PySpark)

### What We Processed
- **20,000,000+** user ratings (`rating.csv` — ~690 MB)
- **27,000+** movies (`movie.csv`)

### PySpark Operations
```python
# Distributed aggregation across all CPU cores
spark = SparkSession.builder \
    .appName("MovieLens20M_Processor") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# GroupBy + Aggregate
movie_stats = ratings_df.groupBy("MovieID").agg(
    avg("Rating").alias("AvgRating"),
    count("Rating").alias("NumRatings")
).filter(col("NumRatings") >= 50)

# Join with movie metadata
final_df = movies_df.join(movie_stats, "MovieID", "inner")
```

### IMDB Weighted Rating Formula

$$\text{Weighted Rating} = \left(\frac{v}{v+m} \cdot R\right) + \left(\frac{m}{v+m} \cdot C\right)$$

Where:
- **v** = Number of votes for the movie
- **m** = Minimum votes required (75th percentile threshold)
- **R** = Average rating of the movie
- **C** = Mean rating across all movies

**Why?** This prevents movies with very few votes (e.g., 1 rating of 5.0★) from outranking widely acclaimed films.

---

## 📌 Slide 7 — HBase NoSQL Database Design

### Table: `kino_movies`

| Column Family | Columns | Data Type | Purpose |
|---|---|---|---|
| **`info`** | `info:title`, `info:genres` | String | Immutable movie metadata |
| **`stats`** | `stats:avg_rating`, `stats:num_ratings`, `stats:weighted_rating` | Float/Int | Computed statistics from PySpark |
| **`links`** | `links:poster_url`, `links:backdrop_url`, `links:overview` | String | External data from TMDb API |

### Row Key Design
- **Zero-padded MovieID** (e.g., `000318` for MovieID 318)
- Ensures correct **lexicographical sorting** in HBase scans
- Without padding: `1, 10, 100, 11, 2` → With padding: `000001, 000002, 000010, 000011, 000100`

### Key Operations
- **Seeding:** Batch writes of 100 rows at a time using `table.batch()` to minimize network round-trips
- **Loading:** Full table scan with `batch_size=1000` for fast startup
- **Fallback:** Automatic CSV fallback if HBase is unavailable (fault-tolerant architecture)

---

## 📌 Slide 8 — TMDb API Integration & Concurrency

### Multi-Threaded Poster Fetching

```python
MAX_WORKERS = 30  # 30 concurrent API threads

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_movie = {
        executor.submit(worker, item): item 
        for item in to_fetch
    }
    for future in as_completed(future_to_movie):
        index, tmdb_id, details = future.result()
        # Process result...
```

### Performance Gain
| Approach | Time for ~7,000 movies |
|----------|----------------------|
| Sequential (1 thread) | ~3.5 hours |
| **Concurrent (30 threads)** | **~5–15 minutes** |

### Features
- **MovieLens → TMDb ID mapping** via `link.csv`
- **Rate-limit handling** with `Retry-After` header support
- **Persistent JSON cache** (`poster_cache.json`) — re-runs skip already fetched movies
- **Thread-safe cache updates** using `threading.Lock()`

---

## 📌 Slide 9 — Mood-Based NLP Search

### How It Works
Users type natural language queries → keywords are mapped to movie genres.

```
User Input: "I feel like watching something scary tonight"
Extracted Keyword: "scary"
Mapped Genres: ["Horror", "Thriller"]
→ Returns top Horror/Thriller movies sorted by weighted rating
```

### Mood-to-Genre Mapping (60+ keywords)

| Mood Keywords | Mapped Genres |
|---|---|
| happy, joy, laugh, funny | Comedy, Animation, Musical |
| sad, cry, emotional | Drama, Romance |
| scary, horror, fear, spooky | Horror, Thriller, Fantasy |
| love, romantic, date | Romance, Comedy |
| action, adrenaline, hero | Action, Adventure |
| space, alien, robot, future | Sci-Fi |
| magic, dragon, wizard | Fantasy, Adventure |
| mystery, detective, crime | Mystery, Crime, Thriller |

### Combined Search Strategy
1. **Fuzzy title matching** — handles typos and partial matches using `difflib.SequenceMatcher`
2. **Mood/genre matching** — NLP keyword extraction
3. **Combined scoring** — Title relevance + mood relevance, ranked by weighted rating

---

## 📌 Slide 10 — Backend API Design (FastAPI)

### RESTful Endpoints

| Method | Endpoint | Description |
|--------|---------|-------------|
| `GET` | `/api/trending` | Top-rated movies by weighted rating |
| `GET` | `/api/mood?q=<query>` | Mood/title search with NLP + fuzzy matching |
| `GET` | `/api/genres` | List all available genres |
| `GET` | `/api/movies/genre/{genre}` | Movies filtered by genre |
| `GET` | `/api/movies/{movie_id}` | Movie details + similar movie recommendations |
| `GET` | `/api/hero` | Random featured movie (hero banner) |
| `GET` | `/` | Serve Netflix-themed frontend |

### Dual Data Source Architecture
```python
# Priority 1: Try HBase
hbase_df = load_from_hbase()

if hbase_df is not None:
    movies_df = hbase_df  # ✓ Loaded from NoSQL

else:
    # Priority 2: Fallback to CSV
    movies_df = pd.read_csv("data/movies_with_posters.csv")
```

**Why?** Ensures the application is **always functional**, even without Hadoop/HBase infrastructure.

---

## 📌 Slide 11 — Frontend Design

### Netflix-Inspired Dark Theme
- **Glassmorphism** overlays for search and modals
- **Fluid hover animations** on poster cards (scale, shadow, gradient)
- **Hero banner** with dynamic backdrop images
- **Horizontal carousels** for genre-wise movie browsing
- **Mood chips** for one-click mood searches (😊 Happy, 😱 Scary, ❤️ Romantic, etc.)
- **Movie detail modals** with synopsis, rating, and similar movie carousel
- **Star ratings** and vote counts
- **Skeleton loaders** for loading states
- **Responsive design** — works on desktop, tablet, and mobile

### Key UI Sections
1. **Hero Banner** — Full-width featured movie with backdrop
2. **Mood Search** — Natural language input + mood chips
3. **Trending Now** — Top-rated movies carousel
4. **Genre Carousels** — Action, Drama, Comedy, Sci-Fi sections
5. **Genre Filter Chips** — Browse all available genres
6. **Favorites** — Locally saved favorite movies
7. **Movie Detail Modal** — Expanded view with similar recommendations

---

## 📌 Slide 12 — Folder Structure

```
kino/
├── app.py                       # FastAPI backend (1114 lines)
├── run_kino.py                  # Cross-platform executor script
├── requirements.txt             # Python dependencies
│
├── data/
│   ├── movies_with_posters.csv  # Final enriched dataset (~5 MB)
│   └── movies_with_ratings.csv  # Spark output (~1 MB)
│
├── frontend/
│   ├── index.html               # Netflix-themed SPA
│   ├── styles.css               # Custom styling & animations
│   └── script.js                # Modal, search, carousel logic
│
├── spark/
│   └── spark_processor.py       # PySpark 20M rating aggregator
│
├── scripts/
│   ├── fetch_posters.py         # TMDb API poster fetcher (30 threads)
│   └── setup_hbase.py           # HBase table creator & seeder
│
└── docs/
    ├── database_and_deployment_guide.md
    └── hbase_usage_guide.md
```

---

## 📌 Slide 13 — How to Run the Project

### Quick Start
```bash
# 1. Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Run PySpark processor (processes 20M ratings)
python spark/spark_processor.py

# 3. Fetch posters from TMDb (requires .env with API key)
python scripts/fetch_posters.py

# 4. (Optional) Seed HBase database
hbase thrift start -p 9090  # Start Thrift server first
python scripts/setup_hbase.py

# 5. Launch the web server
python app.py
# Open http://localhost:8000
```

### One-Command Executor
```bash
python run_kino.py --all      # Full pipeline + web server
python run_kino.py --serve    # Web server only
python run_kino.py --status   # Check service status
```

---

## 📌 Slide 14 — Key Academic Highlights

| Concept | Implementation |
|---------|---------------|
| **Big Data Processing** | 20M+ ratings processed via PySpark distributed DataFrames |
| **Distributed Computing** | `local[*]` mode utilizing all available CPU cores |
| **IMDB Weighted Rating** | Mathematical formula to eliminate vote-count bias |
| **NoSQL Database** | HBase column-family schema with 3 CFs: `info`, `stats`, `links` |
| **Row Key Design** | Zero-padded keys for correct lexicographic ordering |
| **Batch Operations** | HBase batch writes (100) and scans (1000) for performance |
| **API Integration** | TMDb REST API for real-time poster/metadata enrichment |
| **Concurrency** | Python `ThreadPoolExecutor` — 30 threads for parallel I/O |
| **NLP Search** | Mood-to-genre keyword mapping with 60+ keywords |
| **Fuzzy Matching** | `difflib.SequenceMatcher` for typo-tolerant title search |
| **Fault Tolerance** | Dual-mode serving (HBase → CSV automatic fallback) |
| **Caching** | Persistent JSON cache for TMDb responses |
| **Modern Frontend** | Netflix-style dark SPA with glassmorphism and micro-animations |

---

## 📌 Slide 15 — Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| Processing 690 MB of rating data | PySpark distributed processing with 4 GB driver memory |
| Fetching 7,000+ movie posters individually | Multi-threaded `ThreadPoolExecutor` with 30 workers |
| TMDb API rate limits | `Retry-After` header handling + persistent JSON cache |
| HBase row key sorting issues | Zero-padded 6-digit row keys (`000318`) |
| Application breaks without HBase | Automatic CSV fallback with graceful error handling |
| Search typos and partial queries | Fuzzy matching + token-level scoring + space-insensitive comparison |
| NaN/null values in data pipeline | Multi-stage data cleaning with `fillna()` and type coercion |

---

## 📌 Slide 16 — Future Scope

1. **Collaborative Filtering** — Implement user-to-user or item-to-item recommendations using Spark MLlib ALS.
2. **Content-Based Filtering** — Use TF-IDF on movie overviews and genre vectors for smarter similarity.
3. **User Authentication** — Login system with personalized watch history and recommendations.
4. **Real-Time Streaming** — Use Spark Streaming / Kafka to process live rating events.
5. **Cloud Deployment** — Deploy on AWS EMR / Google Cloud Dataproc for true distributed cluster processing.
6. **Sentiment Analysis** — Analyze user reviews using NLP models (BERT, spaCy) for deeper mood mapping.
7. **Mobile App** — React Native or Flutter frontend for mobile devices.

---

## 📌 Slide 17 — Conclusion

- Successfully built an **end-to-end Big Data movie recommendation system** that processes 20 million+ ratings.
- Demonstrated mastery of **distributed computing (PySpark)**, **NoSQL databases (HBase)**, **concurrent programming**, and **REST API development**.
- Delivered a **production-quality frontend** with Netflix-level UI/UX aesthetics.
- Implemented **fault-tolerant architecture** with HBase → CSV automatic failover.
- The system is **extensible** — ready for collaborative filtering, cloud deployment, and real-time streaming enhancements.

---

## 📌 Slide 18 — Thank You

### 🎬 Kino — Big Data Movie Recommendation System

*"Find your next favorite movie, powered by Big Data."*

**Demo:** `http://localhost:8000`

**Questions?**

---
