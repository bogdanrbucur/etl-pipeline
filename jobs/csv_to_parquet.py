from pyspark.sql import SparkSession
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("CSV to Parquet ETL") \
    .master("spark://spark:7077") \
    .getOrCreate()

print("Starting CSV to Parquet conversion...")

# Container directory containing CSV files (mapped from host in docker-compose.yml)
data_dir = "/opt/spark/data"

# Get all CSV files in the data directory
csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

if not csv_files:
    print("No CSV files found in the data directory!")
    spark.stop()
    exit(1)

print(f"Found {len(csv_files)} CSV file(s): {csv_files}")

# Process each CSV file
for csv_file in csv_files:
    print(f"\n📁 Processing: {csv_file}")
    
    # Read the CSV file
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{data_dir}/{csv_file}")
    
    print(f"Loaded {df.count()} rows from {csv_file}")
    print("Schema:")
    df.printSchema()
    
    # Create parquet filename (replace .csv with .parquet)
    parquet_filename = csv_file.replace('.csv', '.parquet')
    
    # Write to MinIO as Parquet
    print(f"Writing to MinIO as {parquet_filename}...")
    df.write \
        .mode("overwrite") \
        .parquet(f"s3a://bronze/{parquet_filename}")
    
    print(f"✅ Successfully converted {csv_file} to {parquet_filename}")

print(f"\n🎉 All {len(csv_files)} CSV files converted to Parquet!")
spark.stop()