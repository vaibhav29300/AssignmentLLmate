# README: Data Migration from MongoDB

## Overview
This project implements a data migration process using PySpark to transfer and transform data from MongoDB to Delta Lake. The Medallion Architecture is utilized to structure the data into Bronze, Silver, and Gold layers for efficient data management and analytics.

## Project Structure
- **delta_tables/**: Contains the Delta Lake tables organized into Bronze, Silver, and Gold layers.
  - **bronze/**: Raw data ingested from MongoDB.
  - **silver/**: Cleaned and enriched data.
  - **gold/**: Aggregated and optimized data for analytics.
  
## Requirements
- Apache Spark
- MongoDB
- PySpark
- Delta Lake
- Python 3.x

## Setup Instructions

### 1. Create Delta Tables Directory
Run the following command to create the necessary directories for the Delta tables:
```bash
mkdir -p delta_tables/bronze
mkdir -p delta_tables/silver
mkdir -p delta_tables/gold
```

### 2. Initialize Spark Session
The Spark session is configured to connect to MongoDB:
```python
from pyspark.sql import SparkSession
from delta import *

spark = SparkSession.builder \
    .appName("MongoDB to Delta Lake") \
    .config("spark.mongodb.input.uri", "<your_mongodb_uri>") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()
```

### 3. Data Ingestion
Load data from MongoDB collections into Bronze Delta tables:
```python
bronze_orders_df = spark.read.format("mongo").option("uri", "<your_orders_collection_uri>").load()
bronze_customers_df = spark.read.format("mongo").option("uri", "<your_customers_collection_uri>").load()

bronze_orders_df.write.mode("overwrite").parquet("delta_tables/bronze/bronze_orders_parquet")
```

### 4. Data Cleaning and Transformation
Data is cleaned, transformed, and enriched to create Silver Delta tables:
```python
# Convert date strings to Date type
silver_orders_df = bronze_orders_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
silver_customers_df = bronze_customers_df.withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))

# Handle nulls and remove duplicates
silver_orders_df = silver_orders_df.fillna({"status": "Unknown", "total_amount": 0.0}).dropDuplicates(["order_id"])
silver_customers_df = silver_customers_df.fillna({"email": "unknown@example.com", "phone": "000-0000"}).dropDuplicates(["customer_id"])
```

### 5. Data Integration
Integrate enriched orders with customer information and derive additional fields:
```python
enriched_orders_df = silver_orders_flat_df.join(silver_customers_df, on="customer_id", how="left")
enriched_orders_df = enriched_orders_df.withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name"))) \
                                         .withColumn("region", col("address.state"))
```

### 6. Store Processed Data
Write the enriched data to Silver and Gold Delta tables:
```python
enriched_orders_df.write.mode("overwrite").parquet("delta_tables/silver/silver_orders_parquet")
orders_analytics_df.write.mode("overwrite").csv("delta_tables/gold/gold_orders_analytics.csv")
```

## Running the Application
Run the PySpark application using:
```bash
spark-submit mongodb_to_delta_lake.py
```

## Conclusion
This project provides a robust pipeline for migrating and transforming data from MongoDB to Delta Lake, leveraging the Medallion Architecture for structured data management.
