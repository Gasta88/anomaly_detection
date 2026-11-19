from kfp.dsl import Artifact, Input, Output, component


@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "pyarrow"],
)
def preprocess_data(
    input_data: Input[Artifact],
    output_data: Output[Artifact],
):
    """Add calendar features and keep the columns the model needs."""
    import pandas as pd

    df = pd.read_parquet(input_data.path, engine="pyarrow", dtype_backend="pyarrow")

    df["created_at"] = pd.to_datetime(df["created_at"])
    df["day_of_week"] = df["created_at"].dt.dayofweek
    df["day_of_month"] = df["created_at"].dt.day
    df["month"] = df["created_at"].dt.month
    df["quarter"] = df["created_at"].dt.quarter

    df = df.reset_index(drop=True)
    df.to_parquet(output_data.path)
    print(f"Preprocessed {len(df)} rows to {output_data.path}")
