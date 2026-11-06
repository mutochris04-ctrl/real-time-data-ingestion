# Real-Time Data Ingestion Pipeline

A real-time pipeline for e-commerce events (product views and purchases).
Spark Structured Streaming reads incoming CSV files, cleans and validates
the events, and stores them in PostgreSQL.

## Pipeline

Data generator → CSV input folder → Spark Structured Streaming → PostgreSQL

## Status

Pipeline working: events flow from Python, through Spark, into PostgreSQL.

## How to run

1. Install the requirements:
   `pip install -r requirements.txt`
2. Set up the database in pgAdmin:
   - Run `CREATE DATABASE ecommerce_events;` on its own first.
   - Connect to the `ecommerce_events` database.
   - Run the rest of `postgres_setup.sql` to create the tables.
3. Terminal 1 – start Spark:
   ```
   $env:PG_PASSWORD = "your_password"
   python spark_streaming_to_postgres.py
   ```
4. Terminal 2 – generate events:
   `python data_generator.py`
5. Check the results in pgAdmin:
   ```sql
   SELECT COUNT(*) FROM events;
   SELECT * FROM batch_metrics ORDER BY batch_id;
   ```