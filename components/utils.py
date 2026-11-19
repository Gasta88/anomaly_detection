import os

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

load_dotenv()


def env(name, default=None):
    """Read an environment variable, tolerating quoting from .env or make."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().strip("'\"")


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


def get_credentials():
    """Return explicit service-account credentials when configured, else None.

    None lets google-auth fall back to Application Default Credentials.
    """
    if not SERVICE_ACCOUNT_FILE_PATH:
        return None
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


# Variables
ENV = env("ENV", "dev")
PROJECT_ID = env("PROJECT_ID")
REGION = env("REGION", "us-central1")
BUCKET_NAME = env("BUCKET_NAME", f"anomaly-detection-{ENV}")
SERVICE_ACCOUNT_FILE_PATH = env("SERVICE_ACCOUNT_FILE_PATH")
MODEL_NAME = "ocsvm_model"
BQ_DATASET_NAME = env("DATASET_NAME", "anomaly_detection")
BQ_TABLE_NAME = env("INPUT_TABLE_NAME", "new_users_metrics")
BQ_OUTPUT_TABLE_NAME = env("OUTPUT_TABLE_NAME", "predictions")

TRAIN_SQL = f"""
    SELECT created_at, country_code, platform, channel, new_users
    FROM `{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}`
    WHERE created_at < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """
PREDICT_SQL = f"""
    SELECT new_users, created_at, country_code, platform, channel
    FROM `{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}`
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """
