# Performance Metrics

Measured on 2026-11-17 on a local Windows laptop (Spark in local mode, PostgreSQL 17 on port 5432),
after the redo improvements (batch insertion, `persist()`, watermark + window stream, monitoring).

Two sources:
- `batch_metrics` – my own counts for every batch (rows received, valid, rejected, inserted, seconds).
- `stream_progress` – Spark's own numbers from `query.recentProgress` (rows per second, batch duration, writing time).

## Test 1 – Small volume (normal rate)

`python data_generator.py --files 3 --events 50 --delay 5` (run twice)

| Metric | Value |
|---|---|
| Batches | 6 (one CSV file per batch) |
| Rows received | 300 |
| Rows valid | 290 |
| Rows rejected (saved in `rejected_events` with the reason) | 7 |
| Rows inserted | 288 |
| Average batch time (`batch_metrics.seconds`) | 2.182 s |
| Fastest / slowest batch | 0.935 s / 4.700 s |

All batches finished inside the 5-second trigger. The slower ones were the first batches, while Spark was starting up.

## Test 2 – High volume

`python data_generator.py --files 10 --events 1000 --delay 0`
The generator created 10,000 events in 0.18 seconds (about 55,700 events per second), including
195 bad rows and 219 duplicates on purpose.

### Data quality (`batch_metrics`, batches finished after 11:02)

```sql
SELECT COUNT(*), SUM(rows_received), SUM(rows_valid), SUM(rows_rejected), SUM(rows_inserted),
       ROUND(AVG(seconds),3), MAX(seconds)
FROM batch_metrics WHERE finished_at >= '2026-11-17 11:02';
```

| Metric | Value | Check against the generator |
|---|---|---|
| Batches | 10 (`maxFilesPerTrigger = 1`: one file of 1,000 rows per batch) | 10 files |
| Rows received | 10,000 | 10,000 created |
| Rows rejected | 195 | 195 bad rows created – all caught |
| Rows valid | 9,757 | 10,000 − 195 bad − 48 duplicates inside a batch |
| Rows inserted | 9,586 | 9,757 − 171 duplicates already saved in an earlier batch |
| Duplicates removed in total | 48 + 171 = 219 | 219 duplicates created – all caught |
| Average time in `save_batch` | 1.671 s per 1,000 rows | |
| Slowest `save_batch` | 2.182 s | |

### Spark's own numbers (`stream_progress`, recorded after 11:02)

```sql
SELECT query_name, COUNT(*), SUM(num_input_rows), ROUND(AVG(processed_rows_per_second),1),
       MAX(processed_rows_per_second), ROUND(AVG(trigger_duration_ms)), ROUND(AVG(add_batch_ms))
FROM stream_progress WHERE recorded_at >= '2026-11-17 11:02' GROUP BY query_name;
```

| Stream | Batches | Rows | Avg rows/sec | Best rows/sec | Avg whole batch | Avg writing (`addBatch`) |
|---|---|---|---|---|---|---|
| events_to_postgres | 10 | 10,000 | 253.5 | 279.6 | 4,003 ms | 1,742 ms |
| windows_to_postgres | 10 | 10,000 | 207.7 | 238.6 | 4,886 ms | 2,876 ms |

Both streams read exactly 10,000 rows: each batch was read once (after adding `persist()`), and no batch was missed.

## Throughput and latency

- **Spark processing speed:** about **200–280 rows per second** per stream on this laptop.
- **End-to-end throughput** is limited by Spark, not by the generator or the database: the generator can create
  about 55,700 events per second, while Spark handles one 1,000-row file per batch in about 4–5 seconds.
- **Writing to PostgreSQL** (batch insertion with `execute_values`) takes less than half of each batch
  (about 1.7 s of 4.0 s for the events stream), so the database is not the bottleneck.
- **Latency estimate** (file arrives → rows in PostgreSQL): up to the 5-second trigger plus about 4–5 seconds
  of processing, so roughly **under 10 seconds** when Spark keeps up. This is an estimate from the trigger setting
  and the measured batch times. (`latency_seconds` in `events` measures something else: event time → ingestion by Spark,
  and it includes the late events the generator creates on purpose.)

## Is performance within expected limits?

- **Normal rate (Test 1):** yes – every batch finished inside the 5-second trigger.
- **High volume (Test 2):** mostly. On average both streams finished inside 5 seconds, but some batches took
  5.3–5.9 s and Spark logged *"Current batch is falling behind"*. With 10 files arriving at once, a small backlog
  builds up and is processed one file at a time; no data was lost.

## What I learned from the monitoring

- Before `persist()`, Spark's progress showed **150 rows read for a 50-row batch**: `save_batch` used the batch three
  times (count, good rows, bad rows) and Spark read the files again each time. After `persist()`, rows read = rows received.

## Limitations and possible improvements

- Everything runs on one laptop: the two streaming queries share the same CPU.
- `maxFilesPerTrigger = 1` keeps batches small and easy to follow; reading several files per batch, or a longer
  trigger interval, would raise throughput for large backlogs.
- The window stream keeps state (`spark.sql.streaming.stateStore` / shuffle partitions); its checkpoint was created
  with 200 partitions, which is a lot for one laptop. Starting with a fresh checkpoint and fewer partitions could make it faster.
