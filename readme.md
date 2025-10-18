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

### Running Spark Jobs manually

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

#### Environment Variables

Note that the Spark submit command includes several environment variables as configuration parameters:

- `spark.hadoop.fs.s3a.endpoint` - MinIO endpoint URL
- `spark.hadoop.fs.s3a.access.key` - MinIO access key
- `spark.hadoop.fs.s3a.secret.key` - MinIO secret key
- `spark.hadoop.fs.s3a.path.style.access` - Required for MinIO compatibility
- `spark.hadoop.fs.s3a.impl` - S3A filesystem implementation
- `spark.hadoop.fs.s3a.connection.ssl.enabled` - Disable SSL for local development

These configuration parameters are essential for Spark to connect to the MinIO object storage and must be included in every Spark job submission.

### Running Airflow DAGs (Directed Acyclic Graphs)

1. Create DAG files in the `dags` directory. They must have a `.py` extension.
2. Access the Airflow web interface at http://localhost:8082
3. Log in using the credentials specified in the `docker-compose.yml` file (default: username: `admin`, password: `admin`).
4. Enable and trigger the desired DAGs from the Airflow UI.

## Architecture

The ETL pipeline consists of several containerized services working together:

### Workflow Overview

1. **Data Ingestion**: Raw CSV data is placed in the `data` directory
2. **Processing**: Spark jobs transform CSV data to Parquet format
3. **Storage**: Processed data is stored in MinIO object storage buckets
4. **Orchestration**: Airflow manages and schedules the ETL workflows

### Container Services

- **Spark Master**: Coordinates Spark cluster operations and job distribution
- **Spark Worker(s)**: Execute Spark tasks and data processing jobs
- **MinIO**: S3-compatible object storage for data lake (bronze, silver, gold layers)
- **Airflow server**: Web interface for managing DAGs and monitoring workflows
- **Airflow scheduler**: Schedules and triggers DAG runs based on defined intervals or events

### Data Flow

```
Raw CSV Data → Spark Processing → MinIO Storage → Airflow Orchestration
```

The pipeline follows a medallion architecture pattern where data flows from bronze (raw) to silver (cleaned) to gold (aggregated) storage layers in MinIO.