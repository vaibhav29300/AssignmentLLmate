# -*- coding: utf-8 -*-
"""Assignment_Vaibhav.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1qukTugYcnmf_fc_mQfepo9n8tyHDXWmJ
"""

!pip install delta

!mkdir -p delta_tables/bronze
!mkdir -p delta_tables/silver
!mkdir -p delta_tables/gold

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MongoDB to Delta Lake") \
    .config("spark.mongodb.input.uri", "mongodb+srv://llmate-assignment-orders-read-only:cl89aTfXGb4J@order-data.sco2q.mongodb.net/order_data") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Read from MongoDB collections
bronze_orders_df = spark.read.format("mongo").option("uri", "mongodb+srv://llmate-assignment-orders-read-only:cl89aTfXGb4J@order-data.sco2q.mongodb.net/order_data.orders").load()
bronze_customers_df = spark.read.format("mongo").option("uri", "mongodb+srv://llmate-assignment-orders-read-only:cl89aTfXGb4J@order-data.sco2q.mongodb.net/order_data.customers").load()

# Count the number of rows in bronze_orders_df
row_count = bronze_orders_df.count()
print("Number of rows in bronze_orders_df:", row_count)

# Write to Parquet
bronze_orders_df.write.mode("overwrite").parquet("delta_tables/bronze/bronze_orders_parquet")



# Load the Parquet file into a DataFrame
bronze_orders_parquet_df = spark.read.parquet("delta_tables/bronze/bronze_orders_parquet")

# Register as a temporary view
bronze_orders_parquet_df.createOrReplaceTempView("bronze_orders_parquet_view")

# Example SQL query
spark.sql("SELECT * FROM bronze_orders_parquet_view WHERE total_amount > 100").show()

# Query directly from the Parquet path
spark.sql("SELECT * FROM parquet.`delta_tables/bronze/bronze_orders_parquet` WHERE total_amount > 100").show()

bronze_customers_df.write.mode("overwrite").parquet("delta_tables/bronze/bronze_customers_parquet")

#DataCleaning
from pyspark.sql.functions import col, to_date

# Load Bronze tables
bronze_orders_df = spark.read.parquet("delta_tables/bronze/bronze_orders_parquet")
bronze_customers_df = spark.read.parquet("delta_tables/bronze/bronze_customers_parquet")

# Convert date strings to Date type
silver_orders_df = bronze_orders_df \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("shipping_date", to_date(col("shipping_date"), "yyyy-MM-dd"))

silver_customers_df = bronze_customers_df \
    .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))

# Handle nulls (for demonstration, we’ll fill them with defaults)
silver_orders_df = silver_orders_df.fillna({"status": "Unknown", "total_amount": 0.0})
silver_customers_df = silver_customers_df.fillna({"email": "unknown@example.com", "phone": "000-0000"})

# Remove duplicate records
silver_orders_df = silver_orders_df.dropDuplicates(["order_id"])
silver_customers_df = silver_customers_df.dropDuplicates(["customer_id"])

silver_customers_df.show()

silver_orders_df.write.mode("overwrite").parquet("delta_tables/silver/silver_orders_parquet")

silver_customers_df.write.mode("overwrite").parquet("delta_tables/silver/silver_customers_parquet")

spark.sql("SELECT * FROM parquet.`delta_tables/silver/silver_orders_parquet`").show()

# Count the number of rows in bronze_orders_df
row_count = silver_orders_df.count()
print("Number of rows in bronze_orders_df:", row_count)

#DataEnrichment
from pyspark.sql.functions import explode, size

# Compute item_count based on the number of items in the array
bronze_orders_df = bronze_orders_df.withColumn("item_count", size("items"))

# Flatten the items array to create one row per item
flattened_orders_df = bronze_orders_df.withColumn("item", explode("items"))

# Select necessary columns including expanded item fields
flattened_orders_df = flattened_orders_df.select(
    "order_id",
    "customer_id",
    "order_date",
    "shipping_date",
    "status",
    "total_amount",
    "item_count",
    "item.product_id",
    "item.quantity",
    "item.unit_price"
)

# Show the result
flattened_orders_df.show()

#Data Integration
# Load Bronze tables if not already loaded
bronze_orders_df = spark.read.parquet("delta_tables/bronze/bronze_orders_parquet")
bronze_customers_df = spark.read.parquet("delta_tables/bronze/bronze_customers_parquet")

# Join orders and customers on customer_id to enrich order data with customer information
enriched_orders_df = bronze_orders_df.join(bronze_customers_df, on="customer_id", how="left")

# Select relevant columns (optional, to keep only needed fields)
enriched_orders_df = enriched_orders_df.select(
    "order_id", "customer_id", "order_date", "shipping_date", "status", "total_amount",
    "first_name", "last_name", "email", "phone", "address")
# Show the enriched data
enriched_orders_df.show()

from pyspark.sql.functions import concat_ws, col

# Derive customer_name by concatenating first_name and last_name
enriched_orders_df = enriched_orders_df.withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))

# Extract region from address (assuming region is stored in address.state or another specific field in the nested structure)
# Adjust "address.state" based on actual structure
enriched_orders_df = enriched_orders_df.withColumn("region", col("address.state"))

# Select only necessary columns, including the new derived fields
enriched_orders_df = enriched_orders_df.select(
    "order_id", "customer_id", "order_date", "shipping_date", "status",
    "customer_name", "email", "phone", "region"
)

# Show the enriched and transformed data
enriched_orders_df.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import *
bronze_orders_df = spark.read.parquet("delta_tables/bronze/bronze_orders_parquet")
bronze_customers_df = spark.read.parquet("delta_tables/bronze/bronze_customers_parquet")

### Step 1: Data Cleaning
# Convert date strings to Date type
silver_orders_df = bronze_orders_df \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("shipping_date", to_date(col("shipping_date"), "yyyy-MM-dd"))

silver_customers_df = bronze_customers_df \
    .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))

# Handle nulls by filling in default values
silver_orders_df = silver_orders_df.fillna({"status": "Unknown", "total_amount": 0.0})
silver_customers_df = silver_customers_df.fillna({"email": "unknown@example.com", "phone": "000-0000"})

# Remove duplicate records
silver_orders_df = silver_orders_df.dropDuplicates(["order_id"])
silver_customers_df = silver_customers_df.dropDuplicates(["customer_id"])

### Step 2: Data Enrichment
# Compute item_count based on the number of items in the array
silver_orders_df = silver_orders_df.withColumn("item_count", size("items"))

# Flatten items array to create one row per item in the order
silver_orders_flat_df = silver_orders_df.withColumn("item", explode("items"))

# Select necessary columns, including expanded item fields
silver_orders_flat_df = silver_orders_flat_df.select(
    "order_id", "customer_id", "order_date", "shipping_date", "status", "total_amount", "item_count",
    "item.product_id", "item.quantity", "item.unit_price"
)

### Step 3: Data Integration
# Join orders and customers on customer_id to enrich order data with customer information
enriched_orders_df = silver_orders_flat_df.join(silver_customers_df, on="customer_id", how="left")

### Step 4: Derive Additional Fields
# Derive customer_name by concatenating first_name and last_name
enriched_orders_df = enriched_orders_df.withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))

# Extract region from address (assuming region is stored in address.state)
enriched_orders_df = enriched_orders_df.withColumn("region", col("address.state"))

# Select only necessary columns, including new derived fields
enriched_orders_df = enriched_orders_df.select(
    "order_id", "customer_id", "order_date", "shipping_date", "status", "total_amount", "item_count",
    "customer_name", "email", "phone", "region", "product_id", "quantity", "unit_price"
)

### Step 5: Store Processed Data in Silver Delta Tables
# Write enriched orders data to Silver Delta Lake table
enriched_orders_df.write.mode("overwrite").parquet("delta_tables/silver/silver_orders_parquet")
# Write cleansed customers data to Silver Delta Lake table
silver_customers_df.write.mode("overwrite").parquet("delta_tables/silver/silver_customers_parquet")

enriched_orders_df.show()

# Load Silver orders table
silver_orders_df = spark.read.parquet("delta_tables/silver/silver_orders_parquet")

# Select relevant columns and ensure correct format
orders_analytics_df = silver_orders_df.select(
    "order_id",
    "customer_id",
    "order_date",
    "shipping_date",
    "status",
    "total_amount",
    "item_count",
    "customer_name",
    "email",
    "region"
)

# Write to Gold Delta Lake table for orders analytics
orders_analytics_df.write.mode("overwrite").parquet("delta_tables/gold/gold_orders_analytics")

orders_analytics_df.show()

from pyspark.sql.functions import sum, count, avg, min, max

# Aggregate data to compute sales metrics by region
sales_metrics_df = silver_orders_df.groupBy("region").agg(
    sum("total_amount").alias("total_sales"),
    count("order_id").alias("total_orders"),
    avg("total_amount").alias("average_order_val"),
    min("order_date").alias("first_order_date"),
    max("order_date").alias("last_order_date")
)

# Write to Gold Delta Lake table for sales metrics
sales_metrics_df.write.mode("overwrite").parquet("delta_tables/gold/gold_sales_metrics")

sales_metrics_df.show()

# Cache DataFrames in memory for faster repeated access
orders_analytics_df.cache()
sales_metrics_df.cache()

# Ensure each order has a unique identifier based on order_id
orders_analytics_df = orders_analytics_df.dropDuplicates(["order_id"])

# Ensure each order has a unique identifier based on order_id
orders_analytics_df.show()



orders_analytics_df.write.mode("overwrite").parquet("delta_tables/gold/gold_orders_analytics")

sales_metrics_df

# Read the Parquet file
df = spark.read.parquet("delta_tables/gold/gold_sales_metrics")

# Write DataFrame to CSV format and save to local directory (e.g., "output_data")
df.write.option("header", "true").mode("overwrite").csv("/dbfs/FileStore/output_data/sales_metrics_df")

df = spark.read.parquet("delta_tables/gold/gold_orders_analytics")

# Write DataFrame to CSV format and save to local directory (e.g., "output_data")
df.write.option("header", "true").mode("overwrite").csv("/dbfs/FileStore/output_data/gold_orders_analytics_csv")

