from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

# File name from previous job
parquet_filename = "taxi_data.parquet"

# Create Spark session
spark = (
    SparkSession.builder.appName("Bronze to Silver")
    .getOrCreate()
)

print(f"Starting 'Bronze to Silver' job")

# Fetch the parquet file from S3
df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .parquet(f"s3a://bronze/{parquet_filename}")
)

# Remove unnecessary columns
df = df.drop('RateCodeID')
df = df.drop('store_and_fwd_flag')

print("Dropped 'RateCodeID' and 'store_and_fwd_flag' columns")

# Calculate the avg cost per mile and add it as a new column
df = df.withColumn("Fare per mile", df["fare_amount"] / df["trip_distance"])
print(f"Calculated an added 'Fare per mile'")

# Update the et_processed_at timestamp
df = df.withColumn("etl_processed_at", current_timestamp())

# Write the updated DataFrame back to S3 in the silver bucket
print(f"Writing to MinIO as {parquet_filename}...")
df.write.mode("overwrite").parquet(f"s3a://silver/{parquet_filename}")

print(f"✅ Successfully updated {parquet_filename} in the silver bucket")
spark.stop()
