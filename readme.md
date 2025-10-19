# ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Spark, Docker, and Airflow. The pipeline ingests CSV data, processes it with Spark, and stores the results in a MinIO object storage.

The Spark jobs are orchestrated using Apache Airflow, and the processed data can be queried using DuckDB in a Jupyter Notebook.

## Prerequisites

### Environment Variables Setup

1. **Rename the environment template**:
   ```bash
   cp .env.template .env
   ```

2. **Edit `.env` file** with your own credentials

### Data Download

Download the NYC Yellow Taxi Trip Data CSV from (Kaggle)[https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data] 
and place the `yellow_tripdata_2015-01.csv` file in the `data` directory.

### Docker Setup

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Run the following command to start all services with a single Spark worker:

  ```bash
  docker compose up
  ```

To stop the services:

  ```bash
  docker compose down
  ```

To start with 3 workers:

  ```bash
  docker compose up --scale spark-worker=3
  ```

## Service Access

Once the containers are running, access the following interfaces:

- **MinIO Console**: http://localhost:9001
  - Username: *from .env file* (`MINIO_ROOT_USER`)
  - Password: *from .env file* (`MINIO_ROOT_PASSWORD`)
- **Apache Spark**: http://localhost:8080  - Spark Master UI
- **Apache Spark**: http://localhost:4040/jobs/  - Spark Job UI
- **Apache Airflow**: http://localhost:8082
  - Username: *from .env file* (`AIRFLOW_ADMIN_USERNAME`)
  - Password: *from .env file* (`AIRFLOW_ADMIN_PASSWORD`)

## Initial Configuration

### MinIO Setup

1. Access the MinIO console
2. Log in with credentials from your `.env` file
3. Create a new bucket named `bronze`

## Usage

### Running Spark Jobs manually

> **Note**: Credentials are automatically loaded from environment variables.

1. Create a new Python file (e.g., `csv_to_parquet.py`) in the `/jobs` directory.
2. Submit the job to Spark using the following command:
   ```bash
   docker exec spark /opt/spark/bin/spark-submit \
     --master spark://spark:7077 \
     --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
     --conf spark.hadoop.fs.s3a.access.key==${MINIO_ROOT_USER} \
     --conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD} \
     --conf spark.hadoop.fs.s3a.path.style.access=true \
     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
     --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
     /opt/spark/jobs/csv_to_parquet.py
   ```
The `./data` directory is mounted to `/opt/spark/data` in the Spark container, so any CSV files placed in `./data` on the host will be accessible in the container.

The `./jobs` directory on the host machine is mounted to `/opt/spark/jobs/` in the container.

The included job `csv_to_parquet.py` reads CSV files from this directory and writes Parquet files to the MinIO `bronze` bucket.

### Using Airflow to Orchestrate Jobs

1. Create DAG files in the `dags` directory. They must have a `.py` extension.
2. Access the Airflow web interface at http://localhost:8082
3. Log in using credentials from your `.env` file
4. Enable and trigger the desired DAGs from the Airflow UI.

### Viewing the processed data in Juypyter Notebook with DuckDB

Run the `view_silver_data.ipynb` notebook in your local Jupyter environment. DuckDB will be installed via pip if not already present.

>[!NOTE]
>In S3 a folder is created with a .parquet extension because Parquet files are typically stored as a collection of files within a directory structure. When Spark writes Parquet files to S3 (or S3-compatible storage like MinIO), it creates a directory for each dataset, and within that directory, it stores multiple Parquet files. This is done to optimize performance and allow for parallel processing of the data. When the data is read back, database systems treat the entire directory as a single Parquet dataset.

#### Environment Variables

Note that the Spark submit command includes several environment variables as configuration parameters:

- `spark.hadoop.fs.s3a.endpoint` - MinIO endpoint URL
- `spark.hadoop.fs.s3a.access.key` - MinIO access key
- `spark.hadoop.fs.s3a.secret.key` - MinIO secret key
- `spark.hadoop.fs.s3a.path.style.access` - Required for MinIO compatibility
- `spark.hadoop.fs.s3a.impl` - S3A filesystem implementation
- `spark.hadoop.fs.s3a.connection.ssl.enabled` - Disable SSL for local development

These configuration parameters are essential for Spark to connect to the MinIO object storage and must be included in every Spark job submission.

### Secrets Management

.env file is used to manage sensitive information such as access keys and passwords. Ensure that this file is kept secure and not committed to version control. This is only suitable for local development and testing.

For production environments, consider using a dedicated secrets management tool or service to handle sensitive credentials.

The `create_AWS_secrets.sh` script can be used to create secrets in AWS Secrets Manager for MinIO and Airflow credentials. The `aws_secrets_manager_retrieve.py` module provides a helper class to retrieve these secrets programmatically in Python scripts for Spark jobs or Airflow DAGs.

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
4. **Querying**: DuckDB queries Parquet files directly from MinIO
5. **Orchestration**: Airflow manages and schedules the ETL workflows

### Container Services

- **Spark Master**: Coordinates Spark cluster operations and job distribution
- **Spark Worker(s)**: Execute Spark tasks and data processing jobs
- **MinIO**: S3-compatible object storage for data lake (bronze, silver, gold layers)
- **Airflow server**: Web interface for managing DAGs and monitoring workflows
- **Airflow scheduler**: Schedules and triggers DAG runs based on defined intervals or events

### Data Flow

```
Raw CSV Data → Spark Processing → MinIO Storage → DuckDB Queries
              ↑
    Orchestrated by Airflow
```