from kfp.dsl import component, Artifact, Output
@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-bigquery", "pandas", "pyarrow", "db_dtypes"]
)
def extract_data(
    project_id: str,
    query: str,
    output_data: Output[Artifact]
):
    """Extract data from BigQuery."""
    from google.cloud import bigquery
    import pandas as pd


    client = bigquery.Client(project=project_id)
    df = client.query(query).to_dataframe()

    # Save to GCS as intermediate artifact
    df.to_parquet(output_data.path)
    print(f"Saved {len(df)} rows to {output_data.path}")
