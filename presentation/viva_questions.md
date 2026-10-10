# 🎓 Kino — Viva Questions & Answers
### *Most Probable Questions for Project Evaluation*

---

## 📂 Section 1 — Project Overview

### Q1. What is Kino? Give a brief overview of your project.
**A:** Kino is a Big Data movie recommendation system that processes 20 million+ user ratings from the MovieLens 20M dataset using Apache Spark (PySpark). It computes weighted ratings, stores results in an HBase NoSQL database, enriches data with movie posters from the TMDb API, and serves recommendations through a FastAPI backend with a Netflix-inspired dark-theme frontend. Users can search movies by title, mood, or genre.

---

### Q2. What is the main objective of this project?
**A:** The main objective is to demonstrate a complete Big Data pipeline — from processing millions of records using distributed computing (PySpark), to storing results in a NoSQL database (HBase), and serving them through a modern web application with intelligent search capabilities (mood-based NLP search and fuzzy title matching).

---

### Q3. What dataset did you use? Why?
**A:** We used the **MovieLens 20M** dataset by GroupLens Research. It contains:
- **20,000,263 ratings** from 138,493 users across 27,278 movies
- The `rating.csv` file alone is ~690 MB

We chose this dataset because it provides real-world scale (20M+ records) — large enough to justify Big Data technologies like Spark, yet manageable on a single machine in `local[*]` mode.

---

### Q4. How many team members worked on this? Who did what?
**A:** *(Customize this based on your team. Example:)*
- **Member 1:** PySpark processing pipeline and IMDB weighted rating implementation
- **Member 2:** HBase schema design, setup script, and database seeding
- **Member 3:** TMDb API integration, multi-threaded poster fetching, and caching
- **Member 4:** Frontend development (Netflix UI), FastAPI backend, and API endpoints

---

## 📂 Section 2 — Big Data & PySpark

### Q5. Why did you use PySpark instead of plain Python (Pandas)?
**A:** The `rating.csv` file is ~690 MB with 20 million rows. While Pandas *can* load this file, it does so in a single thread on a single core. PySpark uses **distributed DataFrames** that can parallelize the computation across all available CPU cores (`local[*]` mode). In a production Hadoop cluster, this same code would scale to terabytes of data across hundreds of nodes without any code changes. PySpark also provides lazy evaluation and query optimization via Catalyst.

---

### Q6. What Spark operations did you perform?
**A:**
1. **`read.csv()`** — Load 20M ratings and 27K movies with schema inference
2. **`groupBy("MovieID")`** — Group all ratings by movie
3. **`agg(avg("Rating"), count("Rating"))`** — Compute average rating and total votes per movie
4. **`filter(col("NumRatings") >= 50)`** — Remove movies with fewer than 50 votes to eliminate noise
5. **`join(movies_df, "MovieID", "inner")`** — Merge rating statistics with movie metadata (title, genres)
6. **`toPandas()`** — Convert final result to Pandas for IMDB formula calculation and CSV export

---

### Q7. What is the IMDB Weighted Rating formula? Why did you use it?
**A:** The formula is:

$$\text{WR} = \left(\frac{v}{v+m} \cdot R\right) + \left(\frac{m}{v+m} \cdot C\right)$$

Where:
- **v** = number of votes for the movie
- **m** = minimum votes required to be listed (we use the 75th percentile of all vote counts)
- **R** = average rating of the movie
- **C** = mean rating across all movies in the dataset

**Why?** Without this formula, a movie with 1 rating of 5.0★ would rank higher than The Shawshank Redemption (4.4★ with 67,000 votes). The weighted formula pulls low-vote movies toward the global average, ensuring only genuinely popular and well-rated movies rank at the top.

---

### Q8. What does `local[*]` mean in Spark?
**A:** `local[*]` is a Spark master URL that runs Spark locally using **all available CPU cores** on the machine. The `*` means "use as many threads as there are logical cores." For example, on a 8-core machine, Spark will use 8 parallel threads. This is ideal for development and testing. In production, you'd replace it with a YARN or Mesos cluster URL.

---

### Q9. How long does the Spark processing take?
**A:** On a typical laptop (8 GB RAM, 4 cores), processing 20 million ratings takes approximately **2–5 minutes**. The bulk of the time is spent on the initial CSV read and the `groupBy` shuffle operation. We configured `spark.driver.memory=4g` and `spark.sql.shuffle.partitions=8` to optimize for local execution.

---

## 📂 Section 3 — HBase & NoSQL

### Q10. Why did you use HBase instead of MySQL or SQLite?
**A:**
1. **Scalability:** HBase is built on top of HDFS and scales horizontally. It can handle petabytes of data across a cluster. SQL databases scale vertically (bigger server), which is expensive and has limits.
2. **Column-Family Design:** HBase stores data by column families, not rows. This is more efficient when you only need to read a subset of columns (e.g., just `stats:avg_rating` without loading poster URLs).
3. **Hadoop Ecosystem Integration:** Since we already use Spark (a Hadoop ecosystem tool), HBase naturally fits the architecture.
4. **Academic Demonstration:** It demonstrates knowledge of NoSQL column-family databases, which is a key Big Data concept.

---

### Q11. What is the HBase schema you designed? Explain the column families.
**A:** Our table is called `kino_movies` with three column families:

| Column Family | Columns | Rationale |
|---|---|---|
| **`info`** | `title`, `genres` | Immutable movie metadata — rarely changes |
| **`stats`** | `avg_rating`, `num_ratings`, `weighted_rating` | Computed metrics from PySpark — may be recomputed |
| **`links`** | `poster_url`, `backdrop_url`, `overview` | External data from TMDb API — separate I/O pattern |

**Why 3 families?** HBase stores each column family in separate HFiles on disk. By separating static metadata (`info`), computed stats (`stats`), and external links (`links`), reads become more efficient because HBase only needs to read the specific column family being queried.

---

### Q12. Why did you zero-pad the row keys?
**A:** HBase sorts rows **lexicographically** (alphabetical string order), not numerically. Without padding:
- `1, 10, 100, 11, 2, 21, 3` → Wrong order!

With zero-padding to 6 digits:
- `000001, 000002, 000003, 000010, 000011, 000021, 000100` → Correct numerical order!

This ensures that sequential `scan()` operations return movies in proper MovieID order.

---

### Q13. How does Python communicate with HBase?
**A:** Python uses the `happybase` library, which communicates with HBase via the **Thrift protocol**. The data flow is:

```
Python (happybase) → Thrift Server (port 9090) → HBase Master → HDFS
```

The Thrift Server acts as a gateway/translator between Python and HBase's Java-native API. We start it with `hbase thrift start -p 9090`.

---

### Q14. What happens if HBase is not running?
**A:** The application implements a **dual-mode fallback** system:
1. On startup, it first tries to connect to HBase (via `happybase.Connection`)
2. If the connection fails (timeout, refused, or table not found), it catches the exception
3. It then falls back to loading data from the local CSV file (`data/movies_with_posters.csv`)
4. The frontend works identically regardless of the data source

This is a **fault-tolerant architecture** — the application never crashes due to database unavailability.

---

### Q15. What is batch writing? Why did you use it in HBase seeding?
**A:** Instead of writing one row at a time (which would make 7,000+ individual Thrift network calls), we use `table.batch()` to buffer 100 rows and send them in a single network call. This reduces:
- **Network overhead** — fewer round-trips to the server
- **Seeding time** — from minutes to seconds
- **Thrift connection stress** — fewer individual operations

---

### Q16. What role does Zookeeper play in HBase?
**A:** Apache Zookeeper is a **coordination service** that HBase depends on for:
1. **Master election** — If the HBase Master fails, Zookeeper helps elect a new one
2. **Region server tracking** — Keeps track of which RegionServers are alive and which regions they serve
3. **Configuration management** — Stores cluster configuration and table metadata
4. **Client routing** — When `happybase` connects, Zookeeper helps route the request to the correct RegionServer

---

## 📂 Section 4 — TMDb API & Concurrency

### Q17. What is the TMDb API? How did you use it?
**A:** The TMDb (The Movie Database) API is a free REST API that provides movie metadata including posters, backdrops, synopses, and release dates. We used it to:
1. Map MovieLens IDs to TMDb IDs using `link.csv`
2. For each TMDb ID, make a GET request to `https://api.themoviedb.org/3/movie/{id}`
3. Extract `poster_path`, `backdrop_path`, and `overview` from the JSON response
4. Construct full image URLs using TMDb's image CDN (`https://image.tmdb.org/t/p/w500/...`)

---

### Q18. Why did you use ThreadPoolExecutor? How many threads?
**A:** We used `concurrent.futures.ThreadPoolExecutor` with **30 worker threads** because:
1. TMDb API calls are **I/O-bound** (network waiting), not CPU-bound
2. While one thread waits for a network response, the other 29 can make their own requests
3. This reduced total fetching time from **~3.5 hours** (sequential) to **~5–15 minutes** (concurrent)
4. We used `as_completed()` to process results as they arrive, rather than waiting for all to finish

---

### Q19. How did you handle rate limits from TMDb?
**A:** Two strategies:
1. **Retry-After Header:** If TMDb returns HTTP 429 (Too Many Requests), we read the `Retry-After` header and `time.sleep()` for that duration before retrying.
2. **Persistent JSON Cache:** Results are cached in `data/poster_cache.json`. If the script is re-run, already-fetched movies are skipped entirely. This means even if the script is interrupted, progress is not lost.

---

### Q20. What is thread safety? How did you handle it?
**A:** Thread safety means ensuring that shared data isn't corrupted when multiple threads access it simultaneously. We used `threading.Lock()` to protect the shared poster cache dictionary:

```python
cache_lock = Lock()

# Thread-safe cache write
with cache_lock:
    cache[tmdb_id_str] = movie_data
```

Without the lock, two threads could write to the cache simultaneously, potentially corrupting the data or losing entries.

---

## 📂 Section 5 — Backend & API

### Q21. Why did you use FastAPI instead of Flask or Django?
**A:**
1. **Async support** — FastAPI is built on ASGI (Uvicorn), supporting async/await natively for non-blocking I/O
2. **Automatic API docs** — Swagger UI at `/docs` is auto-generated
3. **Type hints & validation** — Uses Pydantic for request/response validation
4. **Performance** — FastAPI is one of the fastest Python web frameworks
5. **Simplicity** — Less boilerplate than Django, more structured than Flask

---

### Q22. How does the mood/NLP search work?
**A:**
1. User types a query (e.g., "I want something scary tonight")
2. The backend tokenizes the query and checks each word against a **mood-to-genre dictionary** (60+ keywords)
3. Keyword "scary" maps to genres `["Horror", "Thriller"]`
4. Simultaneously, fuzzy title matching scores all movie titles against the query
5. Both scores are combined: `final_score = title_score + (genre_match × 50)`
6. Results are sorted by combined score, then by weighted rating
7. Top N results are returned as JSON

---

### Q23. How does the fuzzy matching algorithm work?
**A:** Our `score_movie()` function uses a multi-tier scoring system:
1. **Exact match** → score 100
2. **Exact match (ignoring spaces)** → score 95
3. **Starts with query** → score 90
4. **All tokens match** → score 85
5. **Partial token match** → score 40–60 (proportional to matched tokens)
6. **Fuzzy similarity > 0.75** → score 60–80 (using `difflib.SequenceMatcher`)
7. **No match** → score 0

This handles typos (e.g., "Godfahter" matches "Godfather"), partial queries ("Dark Kn" matches "Dark Knight"), and multi-word queries.

---

### Q24. What API endpoints does the backend expose?
**A:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/trending` | GET | Top 20 movies by weighted rating |
| `/api/mood?q=scary` | GET | Mood + title search with NLP |
| `/api/genres` | GET | List all unique genres |
| `/api/movies/genre/Action` | GET | Top movies in a specific genre |
| `/api/movies/858` | GET | Movie details + 12 similar movies |
| `/api/hero` | GET | Random hero movie from top 20 |
| `/` | GET | Serve the frontend HTML |

---

## 📂 Section 6 — Frontend

### Q25. What UI framework did you use?
**A:** We used **vanilla HTML5, CSS3, and JavaScript** — no React, Vue, or Angular. This was intentional to demonstrate core web development skills. The CSS includes custom animations, glassmorphism effects, responsive grid layouts, and Netflix-inspired design tokens. JavaScript handles API calls, DOM manipulation, carousels, modal logic, and favorites (localStorage).

---

### Q26. How does the similar movies feature work?
**A:** When a user clicks on a movie card:
1. The frontend calls `/api/movies/{movie_id}`
2. The backend takes the movie's genres (first 2)
3. It filters all other movies that share at least one genre
4. Results are sorted by weighted rating
5. Top 12 are returned as `similar` in the JSON response
6. The frontend renders them in a carousel inside the modal

---

### Q27. How do favorites work?
**A:** Favorites are stored in the browser's `localStorage`:
- When a user clicks the favorite button, the movie ID is added to a JSON array in `localStorage`
- On page load, the saved IDs are read and matched against the loaded movie data
- This works entirely client-side — no server or database is needed
- Favorites persist across browser sessions but are device-specific

---

## 📂 Section 7 — Architecture & Design Decisions

### Q28. What design pattern does your backend follow?
**A:** The backend follows a **layered architecture**:
1. **Data Layer** — HBase / CSV data loading at startup
2. **Business Logic Layer** — Mood mapping, fuzzy search, weighted rating computation
3. **API Layer** — FastAPI route handlers that validate input and return JSON
4. **Presentation Layer** — Static file serving for the SPA frontend

It also implements the **Strategy Pattern** for data loading (HBase strategy vs. CSV strategy) with automatic fallback.

---

### Q29. Why is this considered a "Big Data" project?
**A:**
1. **Volume:** 20 million+ records (690 MB raw rating data)
2. **Processing:** Distributed computation using Apache Spark
3. **Storage:** Column-family NoSQL database (HBase) on HDFS
4. **The 3 Vs:** Demonstrates Volume (20M ratings), Variety (structured CSV + semi-structured API JSON + column-family DB), and Velocity (concurrent processing)
5. **Ecosystem:** Uses components from the Hadoop ecosystem (Spark, HBase, Zookeeper, HDFS)

---

### Q30. What is the difference between your `movies_with_ratings.csv` and `movies_with_posters.csv`?
**A:**
- **`movies_with_ratings.csv`** (~1 MB) — Output of PySpark. Contains: MovieID, Title, Genres, AvgRating, NumRatings, WeightedRating
- **`movies_with_posters.csv`** (~5 MB) — Output of the poster fetcher. Contains everything above PLUS: poster_url, backdrop_url, overview, tmdb_id, imdb_id

The pipeline is: `rating.csv → PySpark → movies_with_ratings.csv → TMDb API → movies_with_posters.csv → HBase`

---

## 📂 Section 8 — Deployment & Cross-Platform

### Q31. Can this project run on Windows?
**A:** Yes! The `run_kino.py` cross-platform executor detects the OS (`platform.system()`) and adjusts paths, commands, and instructions accordingly. It handles:
- Virtual environment creation (`Scripts/` on Windows vs `bin/` on macOS/Linux)
- Java/HBase installation guidance per OS
- Port checking for service status
- ANSI color code support on Windows 10+

---

### Q32. What are the project's Python dependencies?
**A:**
| Package | Purpose |
|---------|---------|
| `fastapi` | Web framework for REST API |
| `uvicorn` | ASGI web server |
| `pandas` | In-memory data manipulation |
| `pyspark` | Distributed Big Data processing |
| `requests` | HTTP client for TMDb API |
| `python-dotenv` | Load environment variables from `.env` |
| `happybase` | HBase Thrift client library |
| `joblib` | Optional parallel processing utilities |

---

## 📂 Section 9 — Quick-Fire Questions

### Q33. What is ASGI?
**A:** Asynchronous Server Gateway Interface — a standard for Python async web servers. Uvicorn is an ASGI server that allows FastAPI to handle concurrent requests using `async/await` without blocking.

### Q34. What is Thrift?
**A:** Apache Thrift is a framework for cross-language RPC (Remote Procedure Call). HBase's Thrift Server allows non-Java languages (like Python) to interact with HBase by translating method calls into HBase's native Java API.

### Q35. What is column-family storage?
**A:** Unlike row-based storage (RDBMS) where all columns of a row are stored together, column-family storage groups columns into families. Each family is stored in separate files on disk. This makes reads faster when you only need a subset of columns.

### Q36. What is `difflib.SequenceMatcher`?
**A:** A Python standard library class that computes the similarity ratio (0.0 to 1.0) between two strings using the Ratcliff/Obershelp pattern-matching algorithm. We use it for fuzzy search to handle typos and partial matches.

### Q37. What is a `batch_size` in HBase scanning?
**A:** It specifies how many rows to fetch from HBase in a single network call. `batch_size=1000` means the client fetches 1,000 rows at once instead of one at a time, drastically reducing network round-trips and scan time.

### Q38. What is the `.env` file used for?
**A:** It stores sensitive configuration like the TMDb API key (`TMDB_API_KEY=...`). The `python-dotenv` library loads these variables into `os.environ` at startup. This keeps secrets out of source code and `.gitignore` prevents them from being committed to version control.

### Q39. What is lexicographic sorting?
**A:** String-based alphabetical sorting where characters are compared left-to-right by ASCII/Unicode value. HBase uses this for row key ordering. This is why we zero-pad numeric IDs — so `000002` sorts before `000010` (which wouldn't happen with `2` and `10`).

### Q40. What happens if the TMDb API key is invalid or expired?
**A:** The `fetch_posters.py` script checks for the API key at startup. If missing, it exits with an error message and instructions. If the key is invalid, TMDb returns HTTP 401 (Unauthorized), and the script records an empty poster URL for that movie. The web application still works — movies without posters show a placeholder gradient card instead.

---

## 📂 Section 10 — Bonus/Advanced Questions

### Q41. How would you scale this to a real cluster?
**A:**
1. Replace `local[*]` with a YARN/Mesos cluster URL
2. Deploy HBase across multiple RegionServers on HDFS
3. Use Spark's `write.format("hbase")` connector instead of `toPandas()` export
4. Deploy FastAPI behind Nginx with multiple Gunicorn/Uvicorn workers
5. Use a CDN for poster images instead of direct TMDb URLs

### Q42. What is collaborative filtering? Why didn't you implement it?
**A:** Collaborative filtering recommends movies based on user behavior patterns (e.g., "users who liked Movie A also liked Movie B"). We didn't implement it because:
1. The focus of this project was on Big Data processing and NoSQL integration
2. Our mood-based search provides a different (and arguably more intuitive) UX
3. However, the architecture supports it — Spark MLlib's ALS algorithm can be added as a future enhancement

### Q43. What is the difference between content-based and collaborative filtering?
**A:**
- **Content-based:** Recommends based on item attributes (genres, keywords, overview text similarity). "You liked Action → here are more Action movies."
- **Collaborative:** Recommends based on user behavior patterns. "Users similar to you also liked these movies."
Our current system is content-based (genre matching + mood mapping).

### Q44. How does the hero banner feature work technically?
**A:**
1. The `/api/hero` endpoint filters movies that have a valid `backdrop_url` (starts with "http")
2. From the top 20 by weighted rating, it selects one randomly using `DataFrame.sample(1)`
3. The frontend displays the backdrop as a full-width background image with a gradient overlay
4. On each page refresh, a different hero movie is shown

### Q45. What security considerations did you implement?
**A:**
1. **API key protection** — TMDb key stored in `.env` (not in code), excluded via `.gitignore`
2. **Input sanitization** — `re.escape()` on all user search queries to prevent regex injection
3. **Type validation** — `pd.to_numeric(errors="coerce")` prevents crash on malformed data
4. **Error handling** — Try/except blocks around all HBase and API operations
5. **CORS** — FastAPI can be configured with CORS middleware for production deployment

---

*💡 Tip: Read the answers naturally — don't memorize them verbatim. Understanding the "why" behind each decision is more important than reciting code.*
