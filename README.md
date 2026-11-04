# Real-Time Data Ingestion Pipeline

A real-time pipeline for e-commerce events (product views and purchases).
Spark Structured Streaming reads incoming CSV files, cleans and validates
the events, and stores them in PostgreSQL.

## Planned pipeline

Data generator → CSV input folder → Spark Structured Streaming → PostgreSQL

## Status

Work in progress.