from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,from_json,to_timestamp


KAFKA_SERVER = "kafka:29092"
KAFKA_TOPIC = "order_details"



consumer_order_schema  = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("order_id", StringType(), nullable=False),
    StructField("order_line_id", StringType(), nullable=False),
    StructField("product", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("order_date", TimestampType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    
])



def non_empty_df(batch_df,batch_id):
    if batch_df.isEmpty():
        return
    
    print(f"Batch id: {batch_id}")

    batch_df.show(truncate=False)





def run_consumer():
    spark = SparkSession.builder.appName("Kafka_Consumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        kafka_df = spark.readStream\
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER)\
            .option("startingOffsets", "earliest")\
            .option("subscribe", KAFKA_TOPIC)\
            .load()

    except Exception as e:
        print("Error reading from kafka")
        print(f"Error Details: {e}")

        
    message_df = kafka_df.select(col("value").cast(StringType()).alias("message"))

    parsed_df = message_df.select(from_json(col("message"),consumer_order_schema).alias("data")).select("data.*")

    dedup_df = parsed_df.withWatermark("order_date", "2 minutes")

    dedup_df = dedup_df.dropDuplicatesWithinWatermark(["order_id","order_line_id","order_date"])




    dedup_df = dedup_df.withColumn("total_amount", col("quantity") * col("amount"))


    print("Loading was successful")


    try:
        query = (dedup_df.writeStream\
                .outputMode("append")\
                .foreachBatch(non_empty_df)\
                .option("checkpointLocation", "/tmp/consumer_checkpoint")\
                .start()
            )        
        query.awaitTermination()

    except Exception as e:
        print("Error in streaming order data")
        print(f"Error Details: {e}")
    
    finally:
        if query:
            query.stop()
        
        spark.stop()


if __name__ == "__main__":
    run_consumer()






