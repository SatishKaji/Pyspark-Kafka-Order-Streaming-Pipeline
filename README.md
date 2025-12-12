# Spark Streaming with Kafka (Orders Data Pipeline)

This project implements a real-time data pipeline using **Apache Spark Structured Streaming** and **Apache Kafka**.

The **Producer** application streams order data, combines it with the latest customer information, and publishes the resulting records to a Kafka topic. The **Consumer** application reads from the Kafka topic, performs data cleansing (deduplication) and transformation, and prints the results in batches. 

## Prerequisites

To run this application, the following running environments are required:

* **Apache Spark**
* **Apache Kafka**
* **Python**
* **Required Spark Packages**:
    * `spark-sql-kafka-0-10`

## Project Structure
```text
├── producer/
│   ├── producer.py
│   ├── Dockerfile
│   ├── customer/
│   │   └── customer.csv
│   ├── order/
│   │   ├── order_1.csv
│   │   ├── order_2.csv
│   │   └── order_3.csv
│
├── consumer/
│   ├── consumer.py
│   └── Dockerfile
│
└── docker-compose.yml

```



## Producer Key Features

### 1. Dynamic Customer Refresh
The `process_batch` function checks the modification time of the `customer.csv` file before processing each batch.
* If the file has been updated, the cached `customer_df` is unpersisted, the new data is read, and the DataFrame is re-cached.

### 2. Stream-Static Join
Each incoming micro-batch of order data is joined with the latest static customer DataFrame to enrich the orders before publishing to Kafka.

### 3. Kafka Output
The joined DataFrame is converted to JSON format and published to the `order_details` topic.

## Consumer Key Features

### 1. Data Parsing
The raw Kafka value is parsed into its structured components.

### 2. Watermarking and Deduplication
The consumer uses Spark's built-in Structured Streaming capabilities for stateful processing:
* A **watermark** of **10 minutes** is applied to the `order_date` column.
* `dropDuplicatesWithinWatermark` is used to remove duplicates based on the compound key (`order_id`, `order_line_id`, `order_date`). 

### 3. Transformation
It calculates the final `total_amount` using the expression:
$total\\_amount = quantity \times amount$




