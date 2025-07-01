from kfp.dsl import component
@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-aiplatform"]
    )
def get_model(
    project_id: str,
    location: str,
    model_name: str,
    src_bq_table: str
    ) -> str:
    from google.cloud import aiplatform
    from google.cloud import bigquery

    # Prepare input features for predictions
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{src_bq_table}_tmp` 
    OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR))
    AS
    SELECT created_at, country_code, platform, channel, new_users
    FROM `{project_id}.{src_bq_table}`
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """

    client = bigquery.Client(project=project_id)
    job = client.query(query).result()

    print(f"Created temporary table {project_id}.{src_bq_table}_tmp")

    aiplatform.init(project=project_id, location=location)
    return [
        m.resource_name
        for m in aiplatform.Model.list(order_by="create_time desc")
        if model_name in m.display_name
    ][0]