from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

# Create Spark session
spark = (
    SparkSession.builder.appName("CSV to Parquet ETL")
    .getOrCreate()
)

current_time = datetime.now()
print(f"🚀 Starting CSV to Parquet conversion at {current_time}")

# Container directory containing CSV files (mapped from host in docker-compose.yml)
data_dir = "/opt/spark/data"

# Get all CSV files in the data directory
csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

if not csv_files:
    print("No CSV files found in the data directory!")
    spark.stop()
    exit(1)

print(f"Found {len(csv_files)} CSV file(s): {csv_files}")

# Process each CSV file
for csv_file in csv_files:
    print(f"\n📁 Processing: {csv_file}")

    # Read the CSV file
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{data_dir}/{csv_file}")
    )

    # Add a processing timestamp to prove it's a fresh run
    df_with_timestamp = df.withColumn("etl_processed_at", current_timestamp())

    print(f"Loaded {df_with_timestamp.count()} rows from {csv_file}")
    print("Schema with timestamp:")
    df_with_timestamp.printSchema()

    # Create parquet filename (taxi_data)
    parquet_filename = "taxi_data.parquet"

    # Write to MinIO as Parquet
    print(f"Writing to MinIO as {parquet_filename}...")
    df_with_timestamp.write.mode("overwrite").parquet(
        f"s3a://bronze/{parquet_filename}"
    )

    print(f"✅ Successfully converted {csv_file} to {parquet_filename}")

print(f"\n🎉 All {len(csv_files)} CSV files converted to Parquet!")
spark.stop()
