# Test Cases

Manual test plan for the redo version. Tests 1–10 were run on 2026-11-17 (PostgreSQL tables rebuilt from
`postgres_setup.sql`). Tests marked **To do** still need to be run before the review.

| # | What is tested | Steps | Expected outcome | Actual outcome | Result |
|---|---|---|---|---|---|
| 1 | Generator rate and volume are configurable | `python data_generator.py --files 3 --events 50 --delay 5`, then `--files 10 --events 1000 --delay 0` | 3 files of 50 rows, 5 s apart; then 10 files of 1,000 rows at once | 150 rows in 10.0 s (15 events/s); 10,000 rows in 0.18 s (about 55,700 events/s) | Pass |
| 2 | Spark detects new files, one file per batch | Start Spark, then the generator | One batch per file (`maxFilesPerTrigger = 1`) | 10 files → 10 batches of 1,000 rows | Pass |
| 3 | Bad rows are rejected with a reason | High-volume run; `SUM(rows_rejected)` in `batch_metrics`; `SELECT reason, COUNT(*) FROM rejected_events GROUP BY reason` | Same number as the generator's bad rows | Generator: 195 bad → 195 rejected, each with a reason | Pass |
| 4 | Duplicates are removed | Same run; compare received, valid and inserted | Generator's duplicates = (removed inside a batch) + (blocked by the primary key) | 219 = 48 (`dropDuplicates`) + 171 (`ON CONFLICT DO NOTHING`) | Pass |
| 5 | Batch insertion is fast enough | `AVG(seconds)` in `batch_metrics` for 1,000-row batches | Well under the 5 s trigger | Average 1.671 s, slowest 2.182 s per 1,000 rows | Pass |
| 6 | Spark progress is saved (monitoring) | `SELECT * FROM stream_progress` | One row per batch per stream, rows matching what arrived | 10 rows per stream, 10,000 rows each | Pass |
| 7 | Each batch is read only once | Compare `num_input_rows` (Spark) with `rows_received` (my count) | The same | Before `persist()`: 150 vs 50 (batch read 3 times). After `persist()`: 50 vs 50 and 1,000 vs 1,000 | Pass after fix |
| 8 | Window results are saved with upsert | Run the generator; `SELECT * FROM window_metrics ORDER BY window_start, event_type` | One row per minute and event type; a changed window updates its row (`updated_at` changes) | Terminal shows `Window batch ... windows saved or updated` for every batch | Pass (rows to be checked in pgAdmin) |
| 9 | Indexes exist | `SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'public'` | 7 indexes + 3 primary keys | 7 `idx_...` + `events_pkey`, `rejected_events_pkey`, `window_metrics_pkey` | Pass |
| 10 | Performance under high volume | Compare batch duration with the 5 s trigger (`stream_progress`) | Spark keeps up or catches up without losing data | Average 4.0 s (events) and 4.9 s (windows); some batches 5.3–5.9 s ("falling behind"), all 10,000 rows processed | Pass, with a small backlog |
| 11 | Derived columns are correct | `SELECT event_type, price, quantity, total_amount, is_purchase, event_hour, latency_seconds FROM events LIMIT 10` | `total_amount` = price × quantity for purchases, 0 otherwise; `is_purchase` true only for purchases | – | **To do** |
| 12 | Late events within the watermark | Generator with `--late-percent 20`; check `window_metrics.updated_at` | Windows from a few minutes ago get updated | – | **To do** |
| 13 | Restart continues from the checkpoint | Stop Spark (Ctrl+C), start it again without deleting `checkpoints` | Old files are not processed again; new files are | – | **To do** |
| 14 | Missing input folder | Start Spark when `input_data/` does not exist | Spark tells us the folder is missing | (2026-11-06) Stopped with `PATH_NOT_FOUND: input_data`; fixed with `mkdir input_data` | Pass after fix (in user guide) |
