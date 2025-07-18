from kfp.dsl import component, Artifact, Output, Input, Model
@component(
    base_image="python:3.9",
    packages_to_install=["scikit-learn", "pandas", "pyarrow", "matplotlib", "google-cloud-aiplatform", "google-cloud-storage"]
)
def train_ocsvm_model(
    project_id: str,
    location: str,
    bucket_name: str,
    training_data: Input[Artifact],
    model_output: Output[Model]
):
    """Train One-Class SVM model for anomaly detection."""
    import pandas as pd
    import os
    from sklearn.svm import OneClassSVM
    import joblib
    from google.cloud import aiplatform
    from google.cloud import storage

    aiplatform.init(project=project_id, location=location)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)


    # Load data and metadata
    df = pd.read_parquet(training_data.path, engine='pyarrow', dtype_backend="pyarrow")

    # Get unique values for metadata
    countries = ",".join(df['country_code'].unique())  
    platforms = ",".join(df['platform'].unique())  
    channels = ",".join(df['channel'].unique())

    # Sort by date
    df = df.sort_values('created_at')


    # Create One-Class SVM model
    ocsvm = OneClassSVM(kernel='rbf', gamma=0.1, nu=0.1)
    ocsvm.fit(df.drop(['new_users','country_code', 'platform', 'channel','created_at'], axis=1))
    metadata_dict = {
            "country": countries,
            "platform": platforms,
            "channel": channels,
            "framework": "scikit-learn",
            "algorithm": "One-Class SVM"
        }
    # Save the model
    model_dict = {
        "metadata": metadata_dict,
        "model": ocsvm
    }

    # Save the model
    tmp_model_path = os.path.join( "/tmp", "model.joblib")
    gcs_model_path = os.path.join( model_output.path, "model.joblib").replace(f'/gcs/{bucket_name}/', '')
    with open(tmp_model_path, 'wb') as f:
        joblib.dump(model_dict, f)
    # Upload the file to GCS
    blob = bucket.blob(gcs_model_path)
    blob.upload_from_filename(tmp_model_path)
    
    # Upload the model to Vertex AI Model Registry
    model = aiplatform.Model.upload(
        display_name="ocsvm_model",
        artifact_uri="gs://" + model_output.path.replace('/gcs/', ''),
        serving_container_ports=[8080],
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest"
    )
    print(f"Model {model.display_name} uploaded to Vertex AI Model Registry.")