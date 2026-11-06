# Test Cases

Manual test plan. All tests were run on 2026-11-06 after a clean reset
(empty tables, no checkpoints, empty input folder), with one run of `data_generator.py`.

| # | What is tested | Steps | Expected outcome | Actual outcome | Result |
|---|---|---|---|---|---|
| 1 | CSV files are generated correctly | Run `data_generator.py` | 10 CSV files in `input_data/`, 20 rows each, 22 in every 3rd file | 10 files created; files 3, 6 and 9 had 22 rows; total 206 rows | Pass |
| 2 | Spark detects and processes new files | Start Spark, then the generator | One batch per new file | Batches 0 to 9 printed, one per file | Pass |
| 3 | Bad rows are removed | Check files with 22 rows in `batch_metrics` | `rows_valid` is 1 lower than `rows_received` | received 22 → valid 21 (batches with test data) | Pass |
| 4 | Duplicates are skipped | Same batches | `rows_inserted` is 1 lower than `rows_valid` | valid 21 → inserted 20 | Pass |
| 5 | Data is written to PostgreSQL without errors | `SELECT COUNT(*) FROM events;` | 206 − 3 bad − 3 duplicates = 200 | 200 | Pass |
| 6 | Batch metrics are recorded | `SELECT * FROM batch_metrics ORDER BY batch_id;` | 10 rows, batch_id 0 to 9 | 10 rows: 206 received, 203 valid, 200 inserted | Pass |
| 7 | Empty IDs are rejected | Bad row has `event_id = ""` | Row removed by cleaning | Removed (not in `events`) | Pass |
| 8 | Missing input folder | Start Spark when `input_data/` does not exist | Spark should tell us the folder is missing | Stopped with `PATH_NOT_FOUND: input_data`; fixed with `mkdir input_data`, then Spark started | Pass after fix (documented in user guide) |
| 9 | Restart without checkpoints | Delete `checkpoints/` without emptying `batch_metrics` | Batch numbers restart at 0 | Observed during practice: `batch_id` 7 appeared twice (two different days). Fixed by resetting tables and checkpoints together | Known behaviour (documented in user guide) |
| 10 | Performance within limits | Compare processing time with the 5 s trigger | Each batch finishes well under 5 s | Average 0.695 s, maximum 3.093 s | Pass |
