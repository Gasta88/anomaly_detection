from kfp.dsl import Artifact, Input, component


@component(
    base_image="python:3.9",
    packages_to_install=[
        "scikit-learn",
        "pandas",
        "pyarrow",
        "google-cloud-storage",
        "google-cloud-bigquery",
    ],
)
def detect_anomalies(
    project_id: str,
    location: str,
    model_uri: str,
    infer_data: Input[Artifact],
    raw_infer_data: Input[Artifact],
    dataset_name: str = "anomaly_detection",
    output_table_name: str = "predictions",
):
    """Score each combination with its own model and append labels to BigQuery."""
    import os
    import tempfile

    import joblib
    import pandas as pd
    from google.cloud import bigquery, storage

    key_columns = ["country_code", "platform", "channel"]
    feature_columns = ["new_users", "day_of_week", "day_of_month", "month", "quarter"]

    df = pd.read_parquet(infer_data.path, engine="pyarrow", dtype_backend="pyarrow")
    raw_df = pd.read_parquet(raw_infer_data.path, engine="pyarrow", dtype_backend="pyarrow")
    df = df.reset_index(drop=True)
    raw_df = raw_df.reset_index(drop=True)

    model_location = model_uri.removeprefix("gs://")
    model_bucket_name, model_prefix = model_location.split("/", 1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(model_bucket_name)
    tmp_model_path = os.path.join(tempfile.mkdtemp(), "model.joblib")
    bucket.blob(f"{model_prefix.rstrip('/')}/model.joblib").download_to_filename(tmp_model_path)
    models = joblib.load(tmp_model_path)["models"]

    predictions = pd.Series(index=df.index, dtype="object")
    for key, group in df.groupby(key_columns, sort=False):
        combination = "|".join(str(part) for part in key)
        model = models.get(combination)
        if model is None:
            continue
        predictions.loc[group.index] = model.predict(group[feature_columns].astype(float))

    mapping = {1: "correct", -1: "outlier"}
    raw_df["prediction"] = predictions.map(mapping).fillna("no_model")
    raw_df["created_at"] = pd.to_datetime(raw_df["created_at"]).dt.strftime("%Y-%m-%d")
    data = raw_df.to_dict(orient="records")

    client = bigquery.Client(project=project_id, location=location)
    table_id = f"{dataset_name}.{output_table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("country_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("channel", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("new_users", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("prediction", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()
    print(f"Saved {len(data)} rows to {table_id}")
