# ETL Pipeline

## Prerequisites

### Data Download

Download the NYC Yellow Taxi Trip Data CSV from (Kaggle)[https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data] 
and place the `yellow_tripdata_2015-01.csv` file in the `data` directory.

### Create Required Directories

Create the following directories if they do not exist:

```bash
mkdir -p data
mkdir -p logs
mkdir -p minio-data
```

### Docker Setup

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Run the following command to start all services with a single Spark worker:

  ```bash
  docker compose up
  ```

To stop the services, run:

  ```bash
  docker compose down
  ```

To start with 3 workers, run:

  ```bash
  docker compose up --scale spark-worker=3
  ```

## Service Access

Once the containers are running, access the following interfaces:

- **MinIO Console**: http://localhost:9001 (username: `minio`, password: `minio123`)
- **Apache Spark**: http://localhost:8080  - Spark Master UI
- **Apache Spark**: http://localhost:4040/jobs/  - Spark Job UI
- **Apache Airflow**: http://localhost:8082 (username: `admin`, password: `admin`)

Credentials for MinIO and Airflow are set in the `docker-compose.yml` file.

## Initial Configuration

### MinIO Setup

1. Access the MinIO console
2. Create a new bucket named `bronze`

## Usage

### Spark jobs

1. Create a new Python file (e.g., `csv_to_parquet.py`) in the `/jobs` directory.
2. Submit the job to Spark using the following command:
   ```bash
   docker exec spark /opt/spark/bin/spark-submit \
     --master spark://spark:7077 \
     --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
     --conf spark.hadoop.fs.s3a.access.key=minio \
     --conf spark.hadoop.fs.s3a.secret.key=minio123 \
     --conf spark.hadoop.fs.s3a.path.style.access=true \
     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
     --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
     /opt/spark/jobs/csv_to_parquet.py
   ```
The `./data` directory is mounted to `/opt/spark/data` in the Spark container, so any CSV files placed in `./data` on the host will be accessible in the container.

The `./jobs` directory on the host machine is mounted to `/opt/spark/jobs/` in the container.

The sample job `csv_to_parquet.py` reads CSV files from this directory and writes Parquet files to the MinIO `bronze` bucket.

### Environment Variables

Note that the Spark submit command includes several environment variables as configuration parameters:

- `spark.hadoop.fs.s3a.endpoint` - MinIO endpoint URL
- `spark.hadoop.fs.s3a.access.key` - MinIO access key
- `spark.hadoop.fs.s3a.secret.key` - MinIO secret key
- `spark.hadoop.fs.s3a.path.style.access` - Required for MinIO compatibility
- `spark.hadoop.fs.s3a.impl` - S3A filesystem implementation
- `spark.hadoop.fs.s3a.connection.ssl.enabled` - Disable SSL for local development

These configuration parameters are essential for Spark to connect to the MinIO object storage and must be included in every Spark job submission.

## Architecture

[Add architecture diagram and description here]