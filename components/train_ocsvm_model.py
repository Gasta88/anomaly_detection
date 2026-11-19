from kfp.dsl import Artifact, Input, Model, Output, component


@component(
    base_image="python:3.9",
    packages_to_install=[
        "scikit-learn",
        "pandas",
        "pyarrow",
        "google-cloud-aiplatform",
        "google-cloud-storage",
    ],
)
def train_ocsvm_model(
    project_id: str,
    location: str,
    bucket_name: str,
    training_data: Input[Artifact],
    model_output: Output[Model],
):
    """Fit one One-Class SVM per country_code + platform + channel combination."""
    import os
    import tempfile

    import joblib
    import pandas as pd
    from google.cloud import aiplatform, storage
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.svm import OneClassSVM

    key_columns = ["country_code", "platform", "channel"]
    feature_columns = ["new_users", "day_of_week", "day_of_month", "month", "quarter"]
    min_rows_per_model = 10

    aiplatform.init(project=project_id, location=location)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    df = pd.read_parquet(training_data.path, engine="pyarrow", dtype_backend="pyarrow")
    df = df.sort_values("created_at").reset_index(drop=True)

    models = {}
    skipped = {}
    for key, group in df.groupby(key_columns, sort=True):
        combination = "|".join(str(part) for part in key)
        if len(group) < min_rows_per_model:
            skipped[combination] = len(group)
            continue
        pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("ocsvm", OneClassSVM(kernel="rbf", gamma=0.1, nu=0.1)),
            ]
        )
        pipeline.fit(group[feature_columns].astype(float))
        models[combination] = pipeline

    if not models:
        raise ValueError(
            f"No combination had at least {min_rows_per_model} rows; cannot train a model."
        )

    metadata_dict = {
        "combinations": sorted(models),
        "skipped_combinations": skipped,
        "features": feature_columns,
        "min_rows_per_model": min_rows_per_model,
        "framework": "scikit-learn",
        "algorithm": "One-Class SVM",
    }
    model_dict = {
        "metadata": metadata_dict,
        "models": models,
    }

    tmp_model_path = os.path.join(tempfile.mkdtemp(), "model.joblib")
    gcs_model_path = os.path.join(model_output.path, "model.joblib").replace(
        f"/gcs/{bucket_name}/", ""
    )
    with open(tmp_model_path, "wb") as model_file:
        joblib.dump(model_dict, model_file)
    blob = bucket.blob(gcs_model_path)
    blob.upload_from_filename(tmp_model_path)

    model = aiplatform.Model.upload(
        display_name="ocsvm_model",
        artifact_uri="gs://" + model_output.path.replace("/gcs/", ""),
        serving_container_ports=[8080],
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
    )
    print(f"Trained {len(models)} models; uploaded {model.display_name} to Vertex AI Model Registry.")
