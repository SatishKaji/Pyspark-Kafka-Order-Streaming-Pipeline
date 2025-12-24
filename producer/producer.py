from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_json, struct
import os
from datetime import datetime

KAFKA_SERVER = "kafka:29092"
KAFKA_TOPIC = "order_details"
CUSTOMER_FILE = '/app/customer/customer.csv'
ORDER_FILE = '/app/order'

customer_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("city", StringType(), nullable=True)
])

order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_line_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("order_date", TimestampType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
])

last_customer_mtime = None
customer_df = None


def process_batch(batch_df, batch_id):

    global last_customer_mtime, customer_df
    spark = batch_df.sparkSession

    try:
        current_mtime = os.path.getmtime(CUSTOMER_FILE)
        if last_customer_mtime is None or current_mtime != last_customer_mtime:
            current_mtime_dt = datetime.fromtimestamp(current_mtime)
            print(f"New Customer file detected (modified at {current_mtime_dt}). Reading CSV...")

            if customer_df is not None:
                customer_df.unpersist(blocking=True)

            customer_df = spark.read \
                .option("header", "true") \
                .schema(customer_schema) \
                .csv(CUSTOMER_FILE) \
                .cache()

            last_customer_mtime = current_mtime
            print(f"Customer data refreshed (rows: {customer_df.count()})")

        customer_orders = customer_df.join(
            batch_df,
            on="customer_id",
            how="inner"
        )
        print(f"Batch {batch_id}: Total rows after joining: {customer_orders.count()}")

        kafka_df = customer_orders.select(
            col("customer_id").cast("string").alias("key"),
            to_json(struct(col("*"))).alias("value")
        )

        print(f"Batch {batch_id}: Writing messages to Kafka topic '{KAFKA_TOPIC}'")
        kafka_df.show(truncate=False)

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("topic", KAFKA_TOPIC) \
            .option("kafka.acks", "all") \
            .option("kafka.retries", "5") \
            .option("kafka.enable.idempotence", "true") \
            .save()

    except Exception as e:
        print(f"Batch {batch_id}: Error processing batch")
        print(f"Details: {e}")
        raise e


def run_producer():
    spark = None
    query = None
    try:
        spark = SparkSession.builder.appName("Kafka_Producer").config("spark.sql.shuffle.partitions", "2").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("Spark session started")

        order_df = spark.readStream \
            .option("header", "true") \
            .option("maxFilesPerTrigger", "1") \
            .schema(order_schema) \
            .csv(ORDER_FILE)

        query = order_df.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", "/tmp/producer_checkpoint") \
            .trigger(processingTime="10 seconds") \
            .start()

        print("Streaming query started successfully")
        query.awaitTermination()

    except Exception as e:
        print("Error in Kafka producer streaming job")
        print(f"Details: {e}")

    finally:
        if query:
            query.stop()
            print("Streaming query stopped")
        if customer_df is not None:
            customer_df.unpersist(blocking=True)
        if spark:
            spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    run_producer()
