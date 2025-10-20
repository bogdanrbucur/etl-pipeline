from pyspark.sql import SparkSession
import sys

# Create Spark session
spark = (
    SparkSession.builder.appName("Bronze to Silver")
    .getOrCreate()
)

# Fetch the parquet file from S3 as provided in the command line argument
df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .parquet(sys.argv[1])
)

# Output the row count to stdout
print(df.count())
spark.stop()