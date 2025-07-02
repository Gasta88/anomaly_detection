from kfp.dsl import component, Artifact, Output, Input
@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "scikit-learn", "pyarrow"]
)
def preprocess_data_train(
    data: Input[Artifact],
    output_training_data: Output[Artifact],
    output_metadata: Output[Artifact]
):
    """Preprocess data for anomaly detection."""
    import pandas as pd
    from sklearn.preprocessing import LabelEncoder
    import json

    # Load the data
    df = pd.read_parquet(data.path, engine='pyarrow', dtype_backend="pyarrow")

    # Convert dates to datetime and extract features
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['day_of_week'] = df['created_at'].dt.dayofweek
    df['day_of_month'] = df['created_at'].dt.day
    df['month'] = df['created_at'].dt.month
    df['quarter'] = df['created_at'].dt.quarter

    # Encode categorical variables
    encoders = {}
    for col in ['country_code', 'platform', 'channel']:
        le = LabelEncoder()
        df[f'{col}_encoded'] = le.fit_transform(df[col])
        encoders[col] = {
            'classes': le.classes_.tolist(),
            'mapping': dict(zip(le.classes_, range(len(le.classes_))))
        }

    # Save the preprocessed data
    df.to_parquet(output_training_data.path)

    # Save encoders and other metadata
    with open(output_metadata.path, 'w') as f:
        json.dump({
            'encoders': encoders,
            'feature_columns': [
                'day_of_week', 'day_of_month', 'month', 'quarter',
                'country_code_encoded', 'platform_encoded', 'channel_encoded'
            ],
            'target_column': 'new_users'
        }, f)