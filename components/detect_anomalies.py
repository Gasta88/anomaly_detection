from kfp.dsl import component, Artifact, Input

@component(
    base_image="python:3.9",
    packages_to_install=["scikit-learn", "pandas", "pyarrow", "google-cloud-storage", "google-cloud-bigquery", "numpy"]
)
def detect_anomalies(
    project_id: str,
    model_uri: str,
    bucket_name: str,
    infer_data: Input[Artifact],
    raw_infer_data: Input[Artifact]
):
    """Detect anomalies in new data."""
    from google.cloud import storage
    from google.cloud import bigquery
    import joblib
    import pandas as pd
    import numpy as np

    # Load infer data 
    df = pd.read_parquet(infer_data.path, engine='pyarrow', dtype_backend="pyarrow")
    # Load raw infer data 
    raw_df = pd.read_parquet(raw_infer_data.path, engine='pyarrow', dtype_backend="pyarrow")

    # Load the model from GCS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(f"{model_uri.split(bucket_name)[-1][1:]}/model.joblib")
    blob.download_to_filename('model.joblib')

    # Load the model using joblib
    ocsvm = joblib.load('model.joblib')['model']

    # Predict anomalies
    predictions = ocsvm.predict(df)

    # Attach predictions to raw infer data
    mapping = {1: 'correct', -1: 'outlier'}
    prediction_labels = np.vectorize(mapping.get)(predictions)
    raw_df['prediction'] = prediction_labels

    # Save to BigQuery table
    client = bigquery.Client()
    table_id = f"{project_id}.anomaly_detection.predictions"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("country_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("channel", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("new_users", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("prediction", bigquery.enums.SqlTypeNames.STRING)
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = client.load_table_from_dataframe(raw_df, table_id, job_config=job_config)
    job.result()

