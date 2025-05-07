from kfp.dsl import component, Artifact, Output, Input, Model
@component(
    base_image="python:3.9",
    packages_to_install=["scikit-learn", "pandas", "pyarrow", "matplotlib"]
)
def train_ocsvm_model(
    training_data: Input[Artifact],
    metadata: Input[Artifact],
    model_output: Output[Model]
):
    """Train One-Class SVM model for anomaly detection."""
    import pandas as pd
    import numpy as np
    import json
    import os
    from sklearn.svm import OneClassSVM
    import pickle

    # Load data and metadata
    df = pd.read_parquet(training_data.path, engine='pyarrow', dtype_backend="pyarrow")
    with open(metadata.path, 'r') as f:
        metadata_dict = json.load(f)

    # Create time series by group
    groups = []
    for country in df['country_code'].unique():
        for platform in df['platform'].unique():
            for channel in df['channel'].unique():
                groups.append((country, platform, channel))


    for country, platform, channel in groups:
        subset = df[(df['country_code'] == country) &
                    (df['platform'] == platform) &
                    (df['channel'] == channel)]

        if len(subset) < 30:  # Skip groups with too little data
            continue

        # Sort by date
        subset = subset.sort_values('created_at')

        # Get time series
        ts_data = subset['new_users'].values.astype(np.float32)

        # Create One-Class SVM model
        ocsvm = OneClassSVM(kernel='rbf', gamma=0.1, nu=0.1)
        ocsvm.fit(ts_data.reshape(-1, 1))
        metadata = {
            "country": country,
            "platform": platform,
            "channel": channel,
            "framework": "scikit-learn",
            "algorithm": "One-Class SVM"
        }
        model_dict = {
            "metadata": metadata,
            "model": ocsvm
        }

        # Save the model
        # model_path = os.path.join( model_output.path, f"{country}_{platform}_{channel}_ocsvm_model.pkl")
        model_path = os.path.join( model_output.path)
        with open(model_path, 'wb') as f:
            pickle.dump(model_dict, f)