from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, to_json, struct
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

def process_batch(batch_df,batch_id):
    global last_customer_mtime,customer_df  
    spark = batch_df.sparkSession 
    
    try:

        current_mtime = os.path.getmtime(CUSTOMER_FILE)
        current_mtime_dt = datetime.fromtimestamp(current_mtime)


        if last_customer_mtime is None or current_mtime != last_customer_mtime:
            print(f"New Customer file found with date modified at {current_mtime_dt}. Reading new Customer file...")

            if customer_df is not None:
                customer_df.unpersist(blocking=True)


            customer_df =spark.read \
                .option("header", "true") \
                .schema(customer_schema) \
                .csv(CUSTOMER_FILE)
            
            customer_df.cache()


            last_customer_mtime = current_mtime
            
            
        print(f"Batch {batch_id}: Successfully refreshed Customer Data (No. of customer: {customer_df.count()}).")


    except Exception as e:
        print(f"Error reading customer csv file")
        print(f"Details: {e}")
        return

    customer_orders = customer_df.join(
            batch_df,
            customer_df.customer_id == batch_df.customer_id,
            "inner").drop(batch_df["customer_id"])
    
    print(f"Total rows after joining : {customer_orders.count()}")
    




    kafka_df = customer_orders.select(
        col("customer_id").cast(StringType()).alias("key"),
        to_json(struct(col("*"))).alias("value")
    )
    
    print(f"Batch {batch_id}: Writing messages to Kafka topic '{KAFKA_TOPIC}'.")

    kafka_df.show(truncate=False)

    try:
        kafka_df.write\
            .format("kafka")\
            .option("kafka.bootstrap.servers", KAFKA_SERVER)\
            .option("topic",KAFKA_TOPIC)\
            .mode("append")\
            .save()
    except Exception as e:
        print(f"Batch:{batch_id}, Error writing to kafka ")
        print(f"Error Details: {e}")


                                          


def run_producer():
    spark = SparkSession.builder.appName("Kafka_Producer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    try:
        order_df = spark.readStream\
                .option("header","True")\
                .option("maxFilesPerTrigger", "1")\
                .schema(order_schema) \
                .csv(ORDER_FILE)



    
    except Exception as e:
        print(f"Error reading order csv file")
        print(f"Details: {e}")
        spark.stop()
        return


    query = None
    try:
        query = order_df.writeStream\
            .foreachBatch(process_batch)\
            .option("checkpointLocation", "/tmp/producer_checkpoint")\
            .trigger(processingTime="10 seconds")\
            .start()
        


        
        print("Stream started successfully")

        query.awaitTermination()



    except Exception as e:
        print("Error in streaming order data")
        print(f"Error : {e}")
    
    finally:
        if query:
            query.stop()

        global customer_df
        if customer_df is not None:
            customer_df.unpersist(blocking=True)

        spark.stop()


if __name__ == "__main__":
    run_producer()

    