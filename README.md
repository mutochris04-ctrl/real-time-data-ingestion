# Real-Time Data Ingestion Pipeline

A real-time pipeline for e-commerce events (product views, add-to-cart and purchases).
Spark Structured Streaming reads incoming CSV files, cleans and validates the events,
adds derived columns, builds per-minute totals with a watermark, and stores everything in PostgreSQL.

## Pipeline

```
data_generator.py ──> input_data/*.csv ──> Spark Structured Streaming ──> PostgreSQL (ecommerce_events)
                                             │
                                             ├─ stream 1: events_to_postgres  -> events, rejected_events, batch_metrics
                                             ├─ stream 2: windows_to_postgres -> window_metrics (watermark + 1-minute window)
                                             └─ monitoring: query.recentProgress -> stream_progress
```

## The pipeline stage by stage

| # | Stage | Where in the code | What it does |
|---|---|---|---|
| 1 | Data generation | `data_generator.py` | Creates CSV files of fake events. Rate and volume are configurable: `--files`, `--events`, `--delay`, `--seed`, and how many rows are bad, duplicated or late (`--bad-percent`, `--duplicate-percent`, `--late-percent`). |
| 2 | Reading the stream (schema on read) | `spark_streaming_to_postgres.py`, step 3 | Spark watches `input_data/` and initially reads the CSV columns as strings. This lets the pipeline inspect incoming values before converting them into the required data types. |
| 3 | Type conversion and validation | `clean_and_type`, `add_reason` | Trims text, converts price, quantity and time to the right types (`try_cast`: a value that does not fit becomes null), and gives every bad row a `reason`. Bad rows go to `rejected_events` instead of being silently dropped. |
| 4 | Transformations (derived columns) | `add_derived_columns` | Adds `total_amount` (price × quantity, purchases only), `is_purchase`, `event_date`, `event_hour`, `ingestion_time` and `latency_seconds` (ingestion_time − event_time: the delay from the event to its ingestion by Spark, not the full time to process and store it). |
| 5 | Persistence (batch insertion) | `save_batch` | Uses batch insertion with `execute_values` instead of inserting each record individually (it sends the rows in pages of up to 1000). `ON CONFLICT (event_id) DO NOTHING` skips duplicates from earlier batches. Each batch is committed as one transaction: all or nothing. |
| 6 | Streaming (watermark + window) | `build_window_stream`, `save_windows` | `window(..., "1 minute")` counts events and revenue per minute and event type. A 10-minute watermark (`withWatermark`) lets Spark account for late-arriving events in these event-time windows, and helps Spark remove old aggregation state. Events arriving later than the watermark may be dropped from the aggregation. Two separate things happen with a late event: **Spark** decides whether it can still count in its window, and if the window result changes, **PostgreSQL** updates the existing row (`ON CONFLICT ... DO UPDATE`, an upsert) instead of adding a second row for the same window. |
| 7 | Monitoring | `save_progress` | Reads Spark's own progress reports (`query.recentProgress`) and saves rows per batch, rows per second and batch duration in `stream_progress`. |
| 8 | Checkpoints | `checkpoints/events`, `checkpoints/windows` | Spark remembers which files it already processed, so after a restart it continues where it stopped. |
| 9 | Database | `postgres_setup.sql` | 5 tables with CHECK rules, primary keys and indexes for the questions I ask most (time, user, type, category, rejection reason, latest progress). |

## How to run

1. Install the requirements:
   `pip install -r requirements.txt`
2. Database settings: copy `.env.example` to `.env` and put your own PostgreSQL password in it.
   (`.env` is in `.gitignore`, so the password never goes to GitHub.)
3. Set up the database in pgAdmin (PostgreSQL on this computer, port 5432):
   - Run `CREATE DATABASE ecommerce_events;` on its own first.
   - Open a Query Tool on `ecommerce_events` and run `postgres_setup.sql`.
4. Terminal 1 – start Spark:
   `python spark_streaming_to_postgres.py`
5. Terminal 2 – generate events (example: 3 files of 50 events, one every 5 seconds):
   `python data_generator.py --files 3 --events 50 --delay 5`
6. Check the results in pgAdmin:
   ```sql
   SELECT COUNT(*) FROM events;
   SELECT reason, COUNT(*) FROM rejected_events GROUP BY reason;
   SELECT * FROM window_metrics ORDER BY window_start, event_type;
   SELECT * FROM batch_metrics ORDER BY batch_id;
   SELECT * FROM stream_progress ORDER BY recorded_at;
   ```

## More documents

- `project_overview.md` – components and design choices
- `user_guide.md` – step-by-step instructions on Windows
- `test_cases.md` – what was tested and the results
- `performance_metrics.md` – measured speed of the pipeline
