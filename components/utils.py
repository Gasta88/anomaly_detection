from google.cloud import storage
import os
from google.oauth2 import service_account

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
PROJECT_ID = "eighth-duality-457819-r4"
REGION = "us-central1"
BUCKET_NAME = f"bondola-ai-anomaly-detection-{ENV}"
SERVICE_ACCOUNT="vertexai@eighth-duality-457819-r4.iam.gserviceaccount.com"
CREDENTIALS = service_account.Credentials.from_service_account_file(
    '/Users/francescogastaldello/Documents/service_accounts/vertexai_eighth-duality-457819-r4-8ebb6feb3b32.json',
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)