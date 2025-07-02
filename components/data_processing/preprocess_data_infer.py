from kfp.dsl import component
@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-bigquery"]
    )
def preprocess_data_infer(
    project_id: str,
    src_bq_table: str
    ) -> str:
    from google.cloud import bigquery

    # Prepare input features for predictions
    dataset_table_name = ".".join(src_bq_table.split(".")[-2:])
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_table_name}_tmp` 
    OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR))
    AS
    SELECT created_at, country_code, platform, channel, new_users
    FROM `{project_id}.{dataset_table_name}`
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """

    client = bigquery.Client(project=project_id)
    job = client.query(query).result()

    print(f"Created temporary table {project_id}.{dataset_table_name}_tmp")
