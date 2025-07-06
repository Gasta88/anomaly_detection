from kfp.dsl import component, Artifact, Output, Input
@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "scikit-learn", "pyarrow"]
)
def preprocess_data(
    data: Input[Artifact],
    mode: str,
    output_data: Output[Artifact],
):
    """Preprocess data for anomaly detection."""
    import pandas as pd
    from sklearn.preprocessing import LabelEncoder

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

    if mode == 'train':
        print("Training mode")
        # Save the preprocessed data
        df.to_parquet(output_data.path)
    else:
        print("Inference mode")
        # Save the preprocessed data
        data.drop(['new_users','country_code', 'platform', 'channel', 'created_at'], axis=1).to_parquet(output_data.path)