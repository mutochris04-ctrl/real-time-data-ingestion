"""Spark Structured Streaming: read e-commerce event files, clean them, and save them in PostgreSQL.

Flow:  input_data/*.csv  ->  Spark (clean + derived columns)  ->  PostgreSQL
       good rows -> events | bad rows -> rejected_events (with the reason) | counts -> batch_metrics
"""
import time

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (col, lit, when, trim, lower, try_to_timestamp,
                                   current_timestamp, to_date, hour, unix_timestamp)

import config   # reads the database settings from the .env file

# 1. Settings
input_folder = "input_data"
checkpoint_folder = "checkpoints/events"

# 2. Start Spark
spark = SparkSession.builder.appName("EcommerceStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 3. Schema on read: every column is read as text first.
#    A bad value (like price "abc") can then never crash the stream; we check it ourselves in step 5.
schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", StringType()),
    StructField("quantity", StringType()),
    StructField("event_time", StringType()),
])

# 4. Read the input folder as a stream (one new file per batch)
events = (spark.readStream
          .schema(schema)
          .option("header", "true")
          .option("maxFilesPerTrigger", 1)
          .csv(input_folder))


# 5. Clean the text and give each column its real type (schema on write)
def clean_and_type(df):
    df = df.withColumn("event_id", trim(col("event_id")))
    df = df.withColumn("user_id", trim(col("user_id")))
    df = df.withColumn("event_type", lower(trim(col("event_type"))))
    df = df.withColumn("product_id", trim(col("product_id")))
    df = df.withColumn("category", lower(trim(col("category"))))
    # try_cast / try_to_timestamp: a bad value becomes empty (null) instead of crashing Spark
    df = df.withColumn("price_number", col("price").try_cast("double"))
    df = df.withColumn("quantity_number", col("quantity").try_cast("int"))
    df = df.withColumn("event_timestamp", try_to_timestamp(col("event_time"), lit("yyyy-MM-dd HH:mm:ss")))
    return df


# 6. Validation rules: write the FIRST problem found in a "reason" column (empty = good row)
def add_reason(df):
    reason = (when(col("event_id").isNull() | (col("event_id") == ""), "missing event_id")
              .when(col("user_id").isNull() | (col("user_id") == ""), "missing user_id")
              .when(col("product_id").isNull() | (col("product_id") == ""), "missing product_id")
              .when(col("event_type").isNull(), "missing event_type")
              .when(~col("event_type").isin("view", "add_to_cart", "purchase"), "unknown event_type")
              .when(col("price_number").isNull(), "price is not a number")
              .when(col("price_number") < 0, "negative price")
              .when(col("quantity_number").isNull() | (col("quantity_number") <= 0), "bad quantity")
              .when(col("event_timestamp").isNull(), "bad event_time"))
    return df.withColumn("reason", reason)


# 7. Derived columns: new information calculated from the existing columns
def add_derived_columns(df):
    df = df.withColumn("total_amount",                        # money only counts for a purchase
                       when(col("event_type") == "purchase", col("price_number") * col("quantity_number"))
                       .otherwise(0))
    df = df.withColumn("is_purchase", col("event_type") == "purchase")
    df = df.withColumn("event_date", to_date(col("event_timestamp")))
    df = df.withColumn("event_hour", hour(col("event_timestamp")))
    df = df.withColumn("ingestion_time", current_timestamp())   # when Spark processed it
    df = df.withColumn("latency_seconds",                        # how late it arrived
                       unix_timestamp(col("ingestion_time")) - unix_timestamp(col("event_timestamp")))
    return df


# 8. What to do with each batch: clean, split good/bad, save in bulk, measure
def save_batch(batch_df, batch_id):
    start = time.time()

    rows_received = batch_df.count()
    if rows_received == 0:
        return

    checked = add_reason(clean_and_type(batch_df))
    good = checked.filter(col("reason").isNull()).dropDuplicates(["event_id"])   # duplicates inside this batch
    bad = checked.filter(col("reason").isNotNull())
    good = add_derived_columns(good)

    good_rows = good.select("event_id", "user_id", "event_type", "product_id", "category",
                            "price_number", "quantity_number", "event_timestamp", "total_amount",
                            "is_purchase", "event_date", "event_hour", "ingestion_time",
                            "latency_seconds").collect()
    bad_rows = bad.select("event_id", "user_id", "event_type", "product_id", "category",
                          "price", "quantity", "event_time", "reason").collect()

    # Turn Spark rows into simple Python lists, ready for the database
    good_values = []
    for row in good_rows:
        good_values.append(list(row))
    bad_values = []
    for row in bad_rows:
        bad_values.append(list(row) + [batch_id])

    connection = psycopg2.connect(host=config.DB_HOST, port=config.DB_PORT, dbname=config.DB_NAME,
                                  user=config.DB_USER, password=config.DB_PASSWORD)
    cursor = connection.cursor()

    # Bulk insert: ONE command for all good rows (instead of one command per row).
    # ON CONFLICT DO NOTHING skips an event_id that is already saved (duplicate from an earlier batch).
    # RETURNING tells us which rows were really inserted.
    inserted = []
    if len(good_values) > 0:
        inserted = execute_values(
            cursor,
            """INSERT INTO events (event_id, user_id, event_type, product_id, category, price, quantity,
                                   event_time, total_amount, is_purchase, event_date, event_hour,
                                   ingestion_time, latency_seconds)
               VALUES %s
               ON CONFLICT (event_id) DO NOTHING
               RETURNING event_id""",
            good_values, page_size=1000, fetch=True)

    if len(bad_values) > 0:
        execute_values(
            cursor,
            """INSERT INTO rejected_events (event_id, user_id, event_type, product_id, category,
                                            price, quantity, event_time, reason, batch_id)
               VALUES %s""",
            bad_values, page_size=1000)

    seconds = round(time.time() - start, 3)

    cursor.execute(
        """INSERT INTO batch_metrics (batch_id, rows_received, rows_valid, rows_rejected, rows_inserted, seconds)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (batch_id, rows_received, len(good_values), len(bad_values), len(inserted), seconds))

    connection.commit()      # all or nothing: the batch is saved completely, or not at all
    cursor.close()
    connection.close()

    print("Batch", batch_id, "| received:", rows_received, "| valid:", len(good_values),
          "| rejected:", len(bad_values), "| inserted:", len(inserted), "| seconds:", seconds)


# 9. Start the stream, with checkpointing (Spark remembers which files are already done)
query = (events.writeStream
         .queryName("events_to_postgres")
         .foreachBatch(save_batch)
         .option("checkpointLocation", checkpoint_folder)
         .trigger(processingTime="5 seconds")
         .start())

print("Streaming started. Waiting for files in", input_folder, "... (Ctrl+C to stop)")

# 10. Keep running
query.awaitTermination()
