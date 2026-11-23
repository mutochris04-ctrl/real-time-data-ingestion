# Project Overview: Real-Time Data Ingestion

This project simulates an e-commerce website that records customer activities, such as viewing products, adding products to a shopping cart, and making purchases.

Python generates simulated events and saves them as CSV files. Apache Spark Structured Streaming continuously monitors the input folder, reads incoming events, validates and cleans the data, creates derived columns, and calculates per-minute event and revenue totals using event-time windows and watermarks.

The processed events, rejected records, window results, and pipeline performance metrics are stored in PostgreSQL.

The project demonstrates data generation, streaming ingestion, data quality validation, transformations, event-time processing, database persistence, and pipeline monitoring.

See `system_architecture.png` for the system architecture diagram.

## Components

| # | Component | Role |
|---|---|---|
| 1 | `data_generator.py` | Creates fake events (event_id, user_id, event_type, product_id, category, price, quantity, event_time) as CSV files in `input_data/`. Rate and volume are settings (`--files`, `--events`, `--delay`, `--seed`); it also adds bad, duplicate and late rows on purpose (`--bad-percent`, `--duplicate-percent`, `--late-percent`) and writes an answer key to `generator_log.csv`. |
| 2 | `input_data/` | The folder Spark watches. Files are first written as `_writing.csv` and then renamed, so Spark never reads a half-written file. |
| 3 | `spark_streaming_to_postgres.py` | Two streaming queries on the same input: `events_to_postgres` (clean rows, rejected rows, batch counts) and `windows_to_postgres` (per-minute totals with a watermark). A loop saves Spark's progress reports. |
| 4 | `config.py` + `.env` | Database settings read from `.env`, so no password is written in the code. |
| 5 | PostgreSQL (`ecommerce_events`) | 5 tables: `events`, `rejected_events`, `window_metrics`, `batch_metrics`, `stream_progress`. |
| 6 | pgAdmin | Used to create the tables and check the results with SQL. |
| – | `checkpoints/` | One folder per stream. Spark's record of which files are done (and the window state), so a restart does not process them twice. |

## Data flow

1. The generator writes a CSV file into `input_data/`.
2. **Schema on read:** Spark initially reads the CSV columns as strings. This lets the pipeline inspect incoming values (e.g. price "abc") before converting them into the required data types.
3. **Type conversion:** `try_cast` turns price, quantity and event_time into numbers and timestamps; a value that does not fit becomes null.
4. **Validation:** `add_reason` writes the first problem found (missing id, unknown event type, negative price, bad quantity, bad time).
   Rows with a reason go to `rejected_events` with that reason, so nothing is lost silently.
5. **Derived columns:** `total_amount` (price × quantity, only for purchases), `is_purchase`, `event_date`, `event_hour`,
   `ingestion_time` and `latency_seconds` (ingestion_time − event_time: the event-to-ingestion delay, not the complete time Spark takes to process and store the event).
6. **Batch insertion:** good rows are saved with `execute_values` instead of one insert per record (it sends them in pages of up to 1000 rows). `ON CONFLICT (event_id) DO NOTHING`
   skips an event that was already saved in an earlier batch. Each batch is committed as one transaction.
7. **Watermark + window:** the second stream keeps only valid rows, removes repeated events (`dropDuplicatesWithinWatermark`)
   and counts events and revenue per 1-minute window and event type. The 10-minute watermark (`withWatermark`) lets Spark
   account for late-arriving events in these event-time windows and helps it remove old aggregation state.
   It does not guarantee every late event is accepted: events later than the watermark may be dropped from the aggregation.
8. **Upsert:** two separate steps. Spark decides whether a late event can still count in its window. If the window result
   changes, PostgreSQL updates the existing row in `window_metrics` (`ON CONFLICT (window_start, event_type) DO UPDATE`)
   instead of adding a second row for the same window.
9. **Monitoring:** every 5 seconds the program reads `query.recentProgress` for both streams and saves
   rows per batch, rows per second and batch duration in `stream_progress`. My own counts per batch go to `batch_metrics`.

## Key design choices

- **Two layers of duplicate protection:** Spark removes duplicates inside a batch; the `event_id` primary key blocks them across batches.
- **Bad rows are kept, with the reason:** easier to explain data quality than rows that just disappear.
- **Late data is handled on purpose:** the generator sends late events; the watermark sets how late an event can be and still
  count in its window; the upsert updates that window's row in PostgreSQL.
- **Each batch is read once:** `persist()` keeps the batch in memory, because `save_batch` uses it three times
  (count, good rows, bad rows). Without it, Spark's progress showed 150 rows read for a 50-row batch.
- **Measurable pipeline:** `batch_metrics` (my counts) and `stream_progress` (Spark's own numbers) prove what happened in every batch.
- **Indexes only where they help:** on the columns I filter by (time, user, type, category, rejection reason, latest progress).
  `window_metrics` needs none extra, because its primary key is already an index. Each index makes reads faster but inserts a little slower.
- **Password outside the code:** in `.env`, which is listed in `.gitignore`.
