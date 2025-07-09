from google.cloud import storage
import os
from google.oauth2 import service_account
from dotenv import load_dotenv
load_dotenv()

#Functions
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the GCS bucket."""
    storage_client = storage.Client()
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    # Create a blob object from the filepath
    blob = bucket.blob(destination_blob_name)
    # Upload the file to GCS
    blob.upload_from_filename(source_file_name)
    return

#Variables
ENV = os.getenv("ENV", "dev")
PROJECT_ID = "my-cool-project-457818"
REGION = "us-central1"
BUCKET_NAME = f"anomaly-detection-{ENV}"
CREDENTIALS = service_account.Credentials.from_service_account_file(
    os.getenv("SERVICE_ACCOUNT_FILE_PATH"),
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)
MODEL_NAME = "ocsvm_model"
BQ_DATASET_NAME = "anomaly_detection"
BQ_TABLE_NAME = "new_users_metrics"

TRAIN_SQL = f"""
    SELECT created_at, country_code, platform, channel, new_users
    FROM `{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}`
    WHERE created_at < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """
PREDICT_SQL =f"""
    SELECT new_users,created_at, country_code, platform, channel
    FROM `{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}`
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """     