# User Guide

Step-by-step instructions to run the pipeline on Windows (PowerShell, VS Code, pgAdmin).

## 1. One-time setup

1. Install Python packages:
   ```
   pip install -r requirements.txt
   ```
2. In pgAdmin, open the Query Tool on the `postgres` database and run only:
   ```sql
   CREATE DATABASE ecommerce_events;
   ```
3. In pgAdmin, click the `ecommerce_events` database, open a new Query Tool, and run the rest of
   `postgres_setup.sql` (the two `CREATE TABLE` statements).
4. Make sure the folder `input_data` exists in the project folder (create it with `mkdir input_data` if not).
   Spark stops with `PATH_NOT_FOUND` if this folder is missing.

## 2. Start from a clean state (optional, recommended before a demo)

1. In pgAdmin (on `ecommerce_events`):
   ```sql
   TRUNCATE events, batch_metrics;
   ```
2. In the terminal, in the project folder:
   ```
   Remove-Item -Recurse -Force checkpoints
   Remove-Item input_data\*
   ```
   Deleting `checkpoints` makes Spark start again from batch 0.

## 3. Run the pipeline

**Terminal 1 – start Spark** (note the colon in `$env:`):
```
$env:PG_PASSWORD = "your_password"
python spark_streaming_to_postgres.py
```
Wait for: `Streaming started. Waiting for files in input_data ... (Ctrl+C to stop)`

**Terminal 2 – generate events** (in VS Code, split the terminal with Ctrl+Shift+5):
```
python data_generator.py
```
Terminal 2 prints one line per file created. Terminal 1 prints one line per batch, for example:
`Batch 5 | received: 22 | valid: 21 | inserted: 20 | seconds: 0.341`

## 4. Verify the results in pgAdmin

```sql
SELECT COUNT(*) FROM events;
SELECT * FROM batch_metrics ORDER BY batch_id;
```
After a clean start and one generator run, `COUNT(*)` should be **200**
(206 generated, minus 3 bad rows, minus 3 duplicates).

## 5. Stop

Press **Ctrl+C** in Terminal 1. The `KeyboardInterrupt` message is normal: it confirms Spark was stopped on purpose.
