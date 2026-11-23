# User Guide

Step-by-step instructions to run the pipeline on Windows (PowerShell, VS Code, pgAdmin,
PostgreSQL 17 installed on this computer, port 5432).

## 1. One-time setup

1. Install Python packages:
   ```
   pip install -r requirements.txt
   ```
2. Database settings: copy `.env.example` to `.env` and write your own PostgreSQL password on the line
   `PG_PASSWORD=` (no spaces, no quotes). `config.py` reads this file. `.env` is in `.gitignore`, so the
   password is never pushed to GitHub.
3. In pgAdmin, under **PostgreSQL 17**, open the Query Tool on the `postgres` database and run only:
   ```sql
   CREATE DATABASE ecommerce_events;
   ```
4. Click the `ecommerce_events` database, open a new Query Tool (**Tools → Query Tool**), paste the whole
   `postgres_setup.sql` and run it (F5). It creates 5 tables (`events`, `rejected_events`, `window_metrics`,
   `batch_metrics`, `stream_progress`) and their indexes.
   Check that the tab title says `ecommerce_events/postgres@PostgreSQL 17` before you run anything.
5. Make sure the folder `input_data` exists in the project folder (create it with `mkdir input_data` if not).
   Spark stops with `PATH_NOT_FOUND` if this folder is missing.

## 2. Start from a clean state (optional, recommended before a demo)

1. In pgAdmin (on `ecommerce_events`):
   ```sql
   TRUNCATE events, rejected_events, window_metrics, batch_metrics, stream_progress;
   ```
2. In the terminal, in the project folder (Spark must be stopped):
   ```
   Remove-Item -Recurse -Force checkpoints
   Remove-Item input_data\*
   ```
   Deleting `checkpoints` makes both streams start again from batch 0.
   Always reset the tables **and** the checkpoints together, otherwise batch numbers repeat in `batch_metrics`.

## 3. Run the pipeline

**Terminal 1 – start Spark:**
```
python spark_streaming_to_postgres.py
```
Wait for: `Streaming started. Waiting for files in input_data ... (Ctrl+C to stop)`

**Terminal 2 – generate events** (click **+** in the Terminal panel to open a second terminal):
```
python data_generator.py --files 3 --events 50 --delay 5
```
Useful settings (see all with `python data_generator.py --help`):

| Setting | Meaning | Default |
|---|---|---|
| `--files` | how many CSV files | 10 |
| `--events` | events in each file | 20 |
| `--delay` | seconds between files (0 = as fast as possible) | 5 |
| `--seed` | same seed = same data every run | none |
| `--bad-percent`, `--duplicate-percent`, `--late-percent` | test data added on purpose | 2, 2, 5 |

High-volume example: `python data_generator.py --files 10 --events 1000 --delay 0`

Terminal 2 prints one line per file and a summary (`Done. Total rows ... | bad ... | duplicates ...`).
Terminal 1 prints three kinds of lines:
```
Batch 15 | received: 50 | valid: 49 | rejected: 1 | inserted: 49 | seconds: 1.037     <- my counts (batch_metrics)
Window batch 17 | windows saved or updated: 6                                       <- window results (window_metrics)
Progress | events_to_postgres | batch 15 | rows: 50 | processed rows/sec: 16.5     <- Spark's numbers (stream_progress)
```

## 4. Verify the results in pgAdmin

```sql
SELECT COUNT(*) FROM events;
SELECT reason, COUNT(*) FROM rejected_events GROUP BY reason;
SELECT * FROM window_metrics ORDER BY window_start, event_type;
SELECT * FROM batch_metrics ORDER BY batch_id;
SELECT * FROM stream_progress ORDER BY recorded_at;
```
Compare with the generator's summary:
- `SUM(rows_rejected)` in `batch_metrics` should equal the generator's **bad** count.
- received − inserted − rejected should equal the generator's **duplicates** count
  (some are removed inside a batch, the rest are blocked by the `event_id` primary key).

To see the indexes:
```sql
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY tablename;
```

## 5. Stop

Press **Ctrl+C** in Terminal 1. The `KeyboardInterrupt` message is normal: it confirms Spark was stopped on purpose.

## 6. Problems I met and how I fixed them

| Message | Cause | Fix |
|---|---|---|
| `password authentication failed for user "postgres"` | `.env` still had the example password | Put the real PostgreSQL password in `.env` |
| `relation "stream_progress" does not exist` | The table was added to `postgres_setup.sql` but not created yet | Run the setup file in pgAdmin on `ecommerce_events` |
| Error on `INSERT INTO events` (line with `execute_values`) | The `events` table was the old version: `CREATE TABLE IF NOT EXISTS` does not change a table that already exists | Drop the old tables and run `postgres_setup.sql` again (this deletes their rows) |
| `PATH_NOT_FOUND: input_data` | Input folder missing | `mkdir input_data` |
| `WARN ... Current batch is falling behind` | A batch took longer than the 5-second trigger (e.g. many files at once) | Not an error: Spark catches up one file at a time |
| `unrecognized arguments` from the generator | Typo in a setting (e.g. `--file3`) | Use `--files 3` (with a space) |
