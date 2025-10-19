#!/bin/bash

# Create MinIO credentials secret
aws secretsmanager create-secret \
    --name etl-pipeline/minio \
    --description "MinIO credentials for ETL pipeline" \
    --secret-string '{
        "username": "minio_prod_user",
        "password": "super_secure_password_123"
    }' \
    --region us-east-1

# Create Airflow credentials secret
aws secretsmanager create-secret \
    --name etl-pipeline/airflow \
    --description "Airflow admin credentials for ETL pipeline" \
    --secret-string '{
        "username": "airflow_admin",
        "password": "airflow_secure_password_456"
    }' \
    --region us-east-1

echo "✅ Secrets created in AWS Secrets Manager"