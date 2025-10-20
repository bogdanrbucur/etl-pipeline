from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator  # Changed from DockerOperator
from airflow.operators.python import PythonOperator
import requests
import os

# Retrieve the MinIO credentials from environment variables
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
S3_ENDPOINT = "http://minio:9000"
SPARK_ENDPOINT = "spark://spark:7077"

# Parquet filename
parquet_file = "taxi_data.parquet"

# Default arguments for the DAG
default_args = {
    "owner": "Bogdan",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    "csv_to_parquet_etl",
    default_args=default_args,
    description="Convert CSV files to Parquet format using Spark",
    schedule_interval=timedelta(days=1),  # Run daily
    # schedule_interval=timedelta(hours=6),  # Every 6 hours
    # schedule_interval='0 2 * * *',        # Daily at 2 AM
    # schedule_interval=None,               # Manual only
    catchup=False,  # Don't run historical dates
    tags=["etl", "spark", "csv", "parquet"],
)


def check_spark_cluster():
    """Check if Spark cluster is healthy before running jobs"""
    try:
        response = requests.get("http://spark:8080/json/", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("aliveworkers", 0) > 0:
                print(f"✅ Spark cluster healthy: {data['aliveworkers']} workers alive")
                return True
            else:
                raise Exception("No alive Spark workers found")
        else:
            raise Exception(f"Spark master not responding: {response.status_code}")
    except Exception as e:
        print(f"❌ Spark cluster check failed: {e}")
        raise


def check_minio_connection():
    """Check if MinIO is accessible"""
    try:
        response = requests.get(f"{S3_ENDPOINT}/minio/health/live", timeout=10)
        if response.status_code == 200:
            print("✅ MinIO is healthy")
            return True
        else:
            raise Exception(f"MinIO health check failed: {response.status_code}")
    except Exception as e:
        print(f"❌ MinIO connection check failed: {e}")
        raise

# PythonOperator to compare counts
def compare_counts():
    with open("/opt/airflow/logs/bronze_count.txt") as f1, open("/opt/airflow/logs/silver_count.txt") as f2:
        bronze = int(f1.read().strip())
        silver = int(f2.read().strip())
    if bronze != silver:
        raise ValueError(f"Row count mismatch! Bronze: {bronze}, Silver: {silver}")
    print(f"✅ Row counts match: {bronze}")

# Task 1: Check Spark cluster health
check_spark_task = PythonOperator(
    task_id="check_spark_cluster",
    python_callable=check_spark_cluster,
    dag=dag,
)

# Task 2: Check MinIO connection
check_minio_task = PythonOperator(
    task_id="check_minio_connection",
    python_callable=check_minio_connection,
    dag=dag,
)

# Task 3: Run Spark ETL job using the existing Spark container (with S3A JARs)
spark_convert_task = BashOperator(
    task_id="csv_to_parquet_spark_job",
    bash_command=f"""
    docker exec spark /opt/spark/bin/spark-submit \
      --master {SPARK_ENDPOINT} \
      --conf spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT} \
      --conf spark.hadoop.fs.s3a.access.key={MINIO_ROOT_USER} \
      --conf spark.hadoop.fs.s3a.secret.key={MINIO_ROOT_PASSWORD} \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/spark/jobs/csv_to_parquet.py
    """,
    dag=dag,
)

# Task 4: Count rows in the Bronze Parquet file
count_bronze_rows_task = BashOperator(
    task_id="count_bronze_rows",
    bash_command=f"""
    docker exec spark /opt/spark/bin/spark-submit \
        --master {SPARK_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.access.key={MINIO_ROOT_USER} \
        --conf spark.hadoop.fs.s3a.secret.key={MINIO_ROOT_PASSWORD} \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        /opt/spark/jobs/count_rows.py \
        s3a://bronze/{parquet_file} > \
        /opt/airflow/logs/bronze_count.txt
    """,
    dag=dag,
)

# Task 5: Run Spark ETL job using the existing Spark container (with S3A JARs)
spark_bronze_to_silver_task = BashOperator(
    task_id="bronze_to_silver_spark_job",
    bash_command=f"""
    docker exec spark /opt/spark/bin/spark-submit \
      --master {SPARK_ENDPOINT} \
      --conf spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT} \
      --conf spark.hadoop.fs.s3a.access.key={MINIO_ROOT_USER} \
      --conf spark.hadoop.fs.s3a.secret.key={MINIO_ROOT_PASSWORD} \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/spark/jobs/bronze_to_silver.py
    """,
    dag=dag,
)

# Task 6: Count rows in the Silver Parquet file
count_silver_rows_task = BashOperator(
    task_id="count_silver_rows",
    bash_command=f"""
    docker exec spark /opt/spark/bin/spark-submit \
        --master {SPARK_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.access.key={MINIO_ROOT_USER} \
        --conf spark.hadoop.fs.s3a.secret.key={MINIO_ROOT_PASSWORD} \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        /opt/spark/jobs/count_rows.py \
        s3a://silver/{parquet_file} > \
        /opt/airflow/logs/silver_count.txt
    """,
    dag=dag,
)

# Task 7: Compare the before and after row counts
compare_counts_task = PythonOperator(
    task_id="compare_row_counts",
    python_callable=compare_counts,
    dag=dag,
)

# Set task dependencies
(
    [check_spark_task, check_minio_task]
    >> spark_convert_task
    >> count_bronze_rows_task
    >> spark_bronze_to_silver_task
    >> count_silver_rows_task
    >> compare_counts_task
)
