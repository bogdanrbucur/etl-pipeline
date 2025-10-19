from pyspark.sql import SparkSession
import os

# Retrieve the MinIO credentials from environment variables (in .env or set by Secrets Manager)
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD')

# Configure Spark to disable problematic S3A features
spark = (
    SparkSession.builder.appName("Test S3 Write")
    .master("spark://spark:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload", "false")
    .config("spark.hadoop.fs.s3a.multipart.uploads.enabled", "false")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
    .config("spark.hadoop.fs.s3a.retry.limit", "0")
    .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "5000")
    .getOrCreate()
)

try:
    # Test simple write
    test_df = spark.createDataFrame([("test", 1), ("data", 2)], ["col1", "col2"])
    test_df.write.mode("overwrite").parquet("s3a://bronze/test.parquet")
    print("✅ S3 write successful!")

except Exception as e:
    print(f"❌ Error: {e}")

spark.stop()
