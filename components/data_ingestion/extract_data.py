from kfp.dsl import component, Artifact, Output
@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-bigquery", "pandas", "pyarrow"]
)
def extract_data(
    project_id: str,
    bq_table: str,
    output_data: Output[Artifact]
):
    """Extract data from BigQuery."""
    from google.cloud import bigquery
    import pandas as pd

    query = f"""
    SELECT created_at, country_code, platform, channel, new_users
    FROM `{project_id}.{bq_table}`
    ORDER BY created_at
    """

    client = bigquery.Client(project=project_id)
    df = client.query(query).to_dataframe()

    # Save to GCS as intermediate artifact
    df.to_parquet(output_data.path)
    print(f"Saved {len(df)} rows to {output_data.path}")
