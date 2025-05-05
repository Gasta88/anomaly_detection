from kfp.v2.dsl import component,Artifact, Input

@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "google-cloud-bigquery", "pyarrow"]
)
def save_anomalies_to_bq(
    project_id: str,
    anomalies: Input[Artifact],
    bq_destination: str
):
    """Save detected anomalies to BigQuery."""
    import pandas as pd
    from google.cloud import bigquery

    # Read anomalies
    anomalies_df = pd.read_csv(anomalies.path)

    if len(anomalies_df) == 0:
        print("No anomalies to save")
        return

    # Save to BigQuery
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("created_at", "DATE"),
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("actual_users", "FLOAT"),
            bigquery.SchemaField("expected_users", "FLOAT"),
            bigquery.SchemaField("lower_bound", "FLOAT"),
            bigquery.SchemaField("upper_bound", "FLOAT"),
            bigquery.SchemaField("z_score", "FLOAT")
        ],
        write_disposition="WRITE_APPEND"
    )

    anomalies_df['created_at'] = pd.to_datetime(anomalies_df['created_at']).dt.date

    job = client.load_table_from_dataframe(
        anomalies_df, bq_destination, job_config=job_config
    )

    job.result()  # Wait for the job to complete
    print(f"Saved {len(anomalies_df)} anomalies to BigQuery")
