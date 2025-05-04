from google.cloud import aiplatform
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import component, pipeline, Artifact, Input, Output

@component(
    base_image="tensorflow/tensorflow:2.10.0",
    packages_to_install=["tensorflow-probability", "pandas", "pyarrow", "google-cloud-bigquery"]
)
def detect_anomalies(
    project_id: str,
    bq_table: str,
    model: Input[Artifact],
    output_anomalies: Output[Artifact]
):
    """Detect anomalies in new data."""
    import pandas as pd
    import numpy as np
    import tensorflow as tf
    import tensorflow_probability as tfp
    import pickle
    import os
    from google.cloud import bigquery

    # Get the latest data from BigQuery
    query = f"""
    SELECT created_at, country_code, platform, channel, new_users
    FROM `{project_id}.{bq_table}`
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY created_at
    """

    client = bigquery.Client(project=project_id)
    new_data = client.query(query).to_dataframe()

    # Load the models
    with open(os.path.join(model.path, 'tfp_anomaly_models.pkl'), 'rb') as f:
        models = pickle.load(f)

    # Process each group
    anomalies = []

    for (country, platform, channel), model_dict in models.items():
        subset = new_data[(new_data['country_code'] == country) &
                          (new_data['platform'] == platform) &
                          (new_data['channel'] == channel)]

        if len(subset) == 0:
            continue

        # Sort by date
        subset = subset.sort_values('created_at')

        # Get time series
        ts_data = subset['new_users'].values.astype(np.float32)

        # Get the model components
        model = model_dict['model']
        variational_posteriors = model_dict['variational_posteriors']

        # Generate forecast
        samples = variational_posteriors.sample(50)
        forecast_dist = tfp.sts.forecast(
            model=model,
            observed_time_series=ts_data,
            parameter_samples=samples,
            num_steps_forecast=0  # 0 means in-sample predictions
        )

        forecast_mean = forecast_dist.mean().numpy()[0]
        forecast_scale = forecast_dist.stddev().numpy()[0]

        # Calculate anomaly scores
        z_scores = np.abs(ts_data - forecast_mean) / forecast_scale

        # Flag anomalies (Z-score > 3)
        for i, (idx, row) in enumerate(subset.iterrows()):
            if z_scores[i] > 3:
                anomalies.append({
                    'created_at': row['created_at'],
                    'country_code': country,
                    'platform': platform,
                    'channel': channel,
                    'actual_users': row['new_users'],
                    'expected_users': forecast_mean[i],
                    'lower_bound': forecast_mean[i] - 2 * forecast_scale[i],
                    'upper_bound': forecast_mean[i] + 2 * forecast_scale[i],
                    'z_score': z_scores[i]
                })

    # Save anomalies to dataframe
    anomalies_df = pd.DataFrame(anomalies)
    anomalies_df.to_csv(output_anomalies.path, index=False)
    print(f"Found {len(anomalies)} anomalies")

@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "google-cloud-bigquery", "pyarrow"]
)
def save_anomalies_to_bq(
    project_id: str,
    anomalies: Input[Artifact],
    bq_destination: str
):
    """Save detected anomalies to BigQuery."""
    import pandas as pd
    from google.cloud import bigquery

    # Read anomalies
    anomalies_df = pd.read_csv(anomalies.path)

    if len(anomalies_df) == 0:
        print("No anomalies to save")
        return

    # Save to BigQuery
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("created_at", "DATE"),
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("actual_users", "FLOAT"),
            bigquery.SchemaField("expected_users", "FLOAT"),
            bigquery.SchemaField("lower_bound", "FLOAT"),
            bigquery.SchemaField("upper_bound", "FLOAT"),
            bigquery.SchemaField("z_score", "FLOAT")
        ],
        write_disposition="WRITE_APPEND"
    )

    anomalies_df['created_at'] = pd.to_datetime(anomalies_df['created_at']).dt.date

    job = client.load_table_from_dataframe(
        anomalies_df, bq_destination, job_config=job_config
    )

    job.result()  # Wait for the job to complete
    print(f"Saved {len(anomalies_df)} anomalies to BigQuery")

@pipeline(name="anomaly-detection-inference-pipeline")
def anomaly_detection_inference(
    project_id: str,
    bq_source_table: str,
    bq_destination_table: str,
    model_name: str
):
    # Get the latest model
    model = aiplatform.Model(model_name)
    model_uri = model.gcs_uri

    # Detect anomalies
    detect_op = detect_anomalies(
        project_id=project_id,
        bq_table=bq_source_table,
        model=model_uri
    )

    # Save anomalies to BigQuery
    save_op = save_anomalies_to_bq(
        project_id=project_id,
        anomalies=detect_op.outputs["output_anomalies"],
        bq_destination=bq_destination_table
    )