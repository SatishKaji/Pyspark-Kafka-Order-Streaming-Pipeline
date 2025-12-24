from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json

KAFKA_SERVER = "kafka:29092"
KAFKA_TOPIC = "order_details"

consumer_order_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("order_id", StringType(), False),
    StructField("order_line_id", StringType(), False),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("quantity", IntegerType(), True),
])

def non_empty_df(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            print(f"Batch {batch_id} is empty, skipping...")
            return
        print(f"Processing Batch id: {batch_id}")          

        batch_df.show(truncate=False)
    except Exception as e:
        print(f"Error in Consumer Batch {batch_id}: {e}")
        raise e
    

def run_consumer():
    spark = None
    try:
        spark = SparkSession.builder.appName("Kafka_Consumer").config("spark.sql.shuffle.partitions", "2").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("Spark session started")

        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("startingOffsets", "earliest") \
            .option("subscribe", KAFKA_TOPIC) \
            .load()

        message_df = kafka_df.select(col("value").cast("string").alias("message"))
        parsed_df = message_df.select(from_json(col("message"), consumer_order_schema).alias("data")).select("data.*")
        dedup_df = parsed_df.withWatermark("order_date", "10 minutes") \
                            .dropDuplicates(["order_id", "order_line_id", "order_date"])
        dedup_df = dedup_df.withColumn("total_amount", col("quantity") * col("amount"))

        query = dedup_df.writeStream \
            .outputMode("append") \
            .foreachBatch(non_empty_df) \
            .option("checkpointLocation", "/tmp/consumer_checkpoint") \
            .start()

        query.awaitTermination()

    except Exception as e:
        print(f"Streaming consumer failed: {e}")

    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    run_consumer()






