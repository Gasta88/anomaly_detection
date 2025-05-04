from google.cloud import storage


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