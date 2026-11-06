import os
import time
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

# 1. Settings
input_folder = "input_data"
checkpoint_folder = "checkpoints/events"

db_host = "localhost"
db_port = 5432
db_name = "ecommerce_events"
db_user = "postgres"
db_password = os.environ.get("PG_PASSWORD")   # set in the terminal, not in the code

# 2. Start Spark
spark = SparkSession.builder.appName("EcommerceStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 3. Schema: the 6 columns we expect
schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("amount", DoubleType())
])

# 4. Read the input folder as a stream (one file per batch)
events = (spark.readStream
          .schema(schema)
          .option("header", "true")
          .option("maxFilesPerTrigger", 1)
          .csv(input_folder))


# 5. Cleaning rules
def clean_events(df):
    df = df.filter(col("event_id").isNotNull() & (col("event_id") != ""))
    df = df.filter(col("user_id").isNotNull() & (col("user_id") != ""))
    df = df.filter(col("product_id").isNotNull() & (col("product_id") != ""))
    df = df.filter(col("event_time").isNotNull())
    df = df.filter(col("event_type").isin("view", "purchase"))
    # a purchase must have an amount above 0  ( | means "or" )
    df = df.filter((col("event_type") == "view") | (col("amount") > 0))
    # remove duplicates inside this batch
    df = df.dropDuplicates(["event_id"])
    return df


# 6. What to do with each batch: clean, save, measure
def save_batch(batch_df, batch_id):
    start = time.time()

    rows_received = batch_df.count()
    if rows_received == 0:
        return

    clean_df = clean_events(batch_df)
    rows = clean_df.collect()
    rows_valid = len(rows)

    connection = psycopg2.connect(host=db_host, port=db_port, dbname=db_name,
                                  user=db_user, password=db_password)
    cursor = connection.cursor()

    rows_inserted = 0
    for row in rows:
        cursor.execute(
            """INSERT INTO events (event_id, user_id, event_type, product_id, event_time, amount)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (event_id) DO NOTHING""",
            (row["event_id"], row["user_id"], row["event_type"],
             row["product_id"], row["event_time"], row["amount"])
        )
        rows_inserted = rows_inserted + cursor.rowcount   # 1 = saved, 0 = duplicate skipped

    seconds = round(time.time() - start, 3)

    cursor.execute(
        """INSERT INTO batch_metrics (batch_id, rows_received, rows_valid, rows_inserted, seconds)
           VALUES (%s, %s, %s, %s, %s)""",
        (batch_id, rows_received, rows_valid, rows_inserted, seconds)
    )

    connection.commit()
    cursor.close()
    connection.close()

    print("Batch", batch_id, "| received:", rows_received, "| valid:", rows_valid,
          "| inserted:", rows_inserted, "| seconds:", seconds)


# 7. Start the stream, with checkpointing
query = (events.writeStream
         .foreachBatch(save_batch)
         .option("checkpointLocation", checkpoint_folder)
         .trigger(processingTime="5 seconds")
         .start())

print("Streaming started. Waiting for files in", input_folder, "... (Ctrl+C to stop)")

# 8. Keep running
query.awaitTermination()
