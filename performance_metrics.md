# Performance Metrics

Measured on 2026-11-06 on a local Windows laptop (Spark in local mode, PostgreSQL 17),
after a clean reset and one run of `data_generator.py`. Source: the `batch_metrics` table.

```sql
SELECT COUNT(*) AS batches, SUM(rows_received), SUM(rows_valid), SUM(rows_inserted),
       ROUND(AVG(seconds), 3), MIN(seconds), MAX(seconds)
FROM batch_metrics;
```

## Results

| Metric | Value |
|---|---|
| Batches processed | 10 (one CSV file per batch) |
| Rows received | 206 |
| Rows valid after cleaning | 203 (3 bad rows removed) |
| Rows inserted into PostgreSQL | 200 (3 duplicates skipped) |
| Data quality: rows kept | 200 / 206 = 97.1 % |
| Average processing time per batch | 0.695 s |
| Fastest batch | 0.334 s |
| Slowest batch | 3.093 s |
| Trigger interval | 5 s |

## Throughput and latency

- **Processing speed:** about 20 rows in 0.695 s, which is roughly **29 rows per second** per batch.
- **End-to-end throughput** is limited by the generator, not by Spark: it creates one file of 20–22 rows
  every 5 seconds (about 4 rows per second). Spark finishes each batch long before the next file arrives.
- **Latency** (from file arriving to rows in PostgreSQL) is at most the trigger interval plus the processing
  time: about 5 s + 0.7 s ≈ **under 6 seconds**. This is an estimate from the trigger setting and the measured
  processing time.
- The slowest batch (3.093 s) is still well inside the 5-second trigger, so the pipeline keeps up with the data.

## Is performance within expected limits?

Yes. Every batch finished in less than the 5-second trigger interval, so no backlog builds up.

## Limitations and possible improvements

- Rows are inserted one by one with psycopg2 after `collect()`. This is fine for small batches;
  for large volumes a bulk insert (for example `execute_values` or Spark's JDBC writer) would be faster.
- Tests used a small volume (206 rows). Throughput with much larger files was not measured.
