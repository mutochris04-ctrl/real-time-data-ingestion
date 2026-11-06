# Project Overview: Real-Time Data Ingestion

This project simulates an e-commerce website that records what customers do (product views and purchases)
and processes those events in near real time with Spark Structured Streaming, storing them in PostgreSQL.

See `system_architecture.png` for the diagram.

## Components

| # | Component | Role |
|---|---|---|
| 1 | `data_generator.py` | Creates fake events (event_id, user_id, event_type, product_id, event_time, amount) and writes one CSV file every 5 seconds into `input_data/`. Every 3rd file also contains one duplicate and one bad row, to test the cleaning. |
| 2 | `input_data/` | The folder Spark watches. Files are first written as `_writing.csv` and then renamed, so Spark never reads a half-written file. |
| 3 | `spark_streaming_to_postgres.py` | Reads new CSV files as a stream (one file per batch, trigger every 5 seconds), applies a fixed schema, cleans and deduplicates the events, and writes each batch to PostgreSQL. |
| 4 | PostgreSQL (`ecommerce_events`) | Stores the events in the `events` table and one row of measurements per batch in `batch_metrics`. |
| 5 | pgAdmin | Used to verify the data with SQL queries. |
| – | `checkpoints/` | Spark's record of which files it has already processed, so a restart does not read them twice. |

## Data flow

1. The generator writes a CSV file into `input_data/`.
2. Spark detects the new file and reads it with a fixed schema (wrong types become null).
3. Cleaning rules remove bad rows: empty or missing IDs, unknown event types (only `view` and `purchase`),
   missing event time, and purchases with an amount of 0 or less.
4. `dropDuplicates(["event_id"])` removes repeated events inside the batch.
5. Each row is inserted with `ON CONFLICT (event_id) DO NOTHING`, so an event that already exists
   from an earlier batch is skipped by the database.
6. Spark records rows received, rows valid, rows inserted and processing time in `batch_metrics`.

## Key design choices

- **Two layers of duplicate protection:** Spark (inside a batch) and the primary key in PostgreSQL (across batches).
- **Password outside the code:** read from the `PG_PASSWORD` environment variable.
- **Measurable pipeline:** `batch_metrics` makes it possible to prove what happened in every batch.
