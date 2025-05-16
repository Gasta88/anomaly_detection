from kfp.dsl import component, Artifact, Input

@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-aiplatform", "google-cloud-bigquery"]
)
def detect_anomalies(
    project_id: str,
    location: str,
    src_bq_table: str,
    dest_bq_table: str,
    model_resource_name: str,
    service_account: str
):
    """Detect anomalies in new data."""
    from google.cloud import bigquery
    from google.cloud import aiplatform

    # Prepare input features for predictions
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{src_bq_table}_tmp` 
    OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
    AS
    SELECT created_at, country_code, platform, channel, new_users
    FROM `{project_id}.{src_bq_table}`
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """

    client = bigquery.Client(project=project_id)
    job = client.query(query).result()

    print(f"Created temporary table {project_id}.{src_bq_table}_tmp")

    # Initialise AI Platform
    aiplatform.init(project=project_id, location=location)

    # Create a batch prediction job
    batch_prediction_job = aiplatform.BatchPredictionJob.create(
            job_display_name="detect-anomalies-batch-prediction",
            model_name=model_resource_name,
            instances_format="bigquery",
            predictions_format="bigquery",
            bigquery_source=f"bq://{project_id}.{src_bq_table}_tmp",
            bigquery_destination_prefix=f"{project_id}.{dest_bq_table.split('.')[0]}",
            model_parameters="",
            machine_type="n1-standard-2",
            project=project_id,
            location=location,
            service_account = service_account
        )

    # Wait for the batch prediction job to complete
    batch_prediction_job.wait()