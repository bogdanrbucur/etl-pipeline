import boto3
import json
from botocore.exceptions import ClientError

# AWS Secrets Manager constants
AWS_REGION = "us-east-1"
MINIO_SECRET_NAME = "etl-pipeline/minio"
AIRFLOW_SECRET_NAME = "etl-pipeline/airflow"
# Add/remove as needed to manage other secrets


class SecretsManager:
    """Helper class to retrieve secrets from AWS Secrets Manager"""

    def __init__(self, region_name=AWS_REGION):
        self.client = boto3.client("secretsmanager", region_name=region_name)

    def get_secret(self, secret_name):
        """
        Retrieve a secret from AWS Secrets Manager

        Args:
            secret_name: Name of the secret to retrieve

        Returns:
            dict: Secret values as dictionary
        """
        try:
            response = self.client.get_secret_value(SecretId=secret_name)

            if "SecretString" in response:
                return json.loads(response["SecretString"])
            else:
                raise ValueError(f"Secret {secret_name} has no SecretString")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                raise ValueError(f"Secret {secret_name} not found")
            elif error_code == "InvalidRequestException":
                raise ValueError(f"Invalid request for secret {secret_name}")
            elif error_code == "InvalidParameterException":
                raise ValueError(f"Invalid parameter for secret {secret_name}")
            else:
                raise e

    def get_minio_credentials(self):
        """Get MinIO credentials"""
        secret = self.get_secret(MINIO_SECRET_NAME)
        return {"username": secret["username"], "password": secret["password"]}

    def get_airflow_credentials(self):
        """Get Airflow credentials"""
        secret = self.get_secret(AIRFLOW_SECRET_NAME)
        return {"username": secret["username"], "password": secret["password"]}


# Example usage
if __name__ == "__main__":
    sm = SecretsManager()

    # Test retrieval
    minio_creds = sm.get_minio_credentials()
    print(f"✅ MinIO username: {minio_creds['username']}")

    airflow_creds = sm.get_airflow_credentials()
    print(f"✅ Airflow username: {airflow_creds['username']}")

# Import the SecretsManager and use it in your DAG or job scripts to fetch credentials securely
# instead of hardcoding them or using .env files.

# For example, in your Spark job submission script, you can do:
# sys.path.append('/opt/spark/scripts')
# from secrets_manager import SecretsManager

# Get credentials from AWS Secrets Manager
# sm = SecretsManager()
# minio_creds = sm.get_minio_credentials()

# Then set the Spark configuration for S3A access using these credentials.

# Create Spark session with credentials from Secrets Manager
# spark = (
#     SparkSession.builder
#     .appName("CSV to Parquet ETL - AWS Secrets")
#     .master("spark://spark:7077")
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#     .config("spark.hadoop.fs.s3a.access.key", minio_creds['username'])
#     .config("spark.hadoop.fs.s3a.secret.key", minio_creds['password'])
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#     .getOrCreate()
# )
