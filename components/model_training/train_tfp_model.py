from kfp.dsl import component, Artifact, Output, Input, Model
@component(
    base_image="tensorflow/tensorflow:2.10.0",
    packages_to_install=["tensorflow-probability", "pandas", "pyarrow", "matplotlib"]
)
def train_tfp_model(
    training_data: Input[Artifact],
    metadata: Input[Artifact],
    model_output: Output[Model]
):
    """Train TensorFlow Probability STS model for anomaly detection."""
    import pandas as pd
    import numpy as np
    import tensorflow as tf
    import tensorflow_probability as tfp
    import json
    import os
    import matplotlib.pyplot as plt

    # Load data and metadata
    df = pd.read_parquet(training_data.path)
    with open(metadata.path, 'r') as f:
        metadata_dict = json.load(f)

    # Create time series by group
    groups = []
    for country in df['country_code'].unique():
        for platform in df['platform'].unique():
            for channel in df['channel'].unique():
                groups.append((country, platform, channel))

    # Train models for each group
    models = {}

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

        # Create TFP model - using Structural Time Series
        tfd = tfp.distributions

        # Build structural time series model
        trend = tfp.sts.LocalLinearTrend(observed_time_series=ts_data)
        seasonal = tfp.sts.Seasonal(
            num_seasons=7,  # Weekly seasonality
            observed_time_series=ts_data
        )
        monthly = tfp.sts.Seasonal(
            num_seasons=30,  # Monthly pattern
            observed_time_series=ts_data
        )

        model = tfp.sts.Sum([trend, seasonal, monthly], observed_time_series=ts_data)

        # Fit the model
        variational_posteriors = tfp.sts.build_factored_surrogate_posterior(model=model)

        # Run variational inference to approximate posterior distributions
        elbo_loss_curve = tfp.vi.fit_surrogate_posterior(
            target_log_prob_fn=model.joint_log_prob(observed_time_series=ts_data),
            surrogate_posterior=variational_posteriors,
            optimizer=tf.optimizers.Adam(learning_rate=0.1),
            num_steps=500
        )

        # Forecast to get bounds
        samples = variational_posteriors.sample(50)
        forecast_dist = tfp.sts.forecast(
            model=model,
            observed_time_series=ts_data,
            parameter_samples=samples,
            num_steps_forecast=1
        )

        forecast_mean = forecast_dist.mean().numpy()
        forecast_scale = forecast_dist.stddev().numpy()

        # Calculate anomaly scores
        anomaly_scores = np.abs(ts_data - forecast_mean[0]) / forecast_scale[0]

        # Generate diagnostic plot
        plt.figure(figsize=(10, 6))
        plt.plot(subset['created_at'], ts_data, 'b-', label='Actual')
        plt.plot(subset['created_at'], forecast_mean[0], 'r-', label='Expected')
        plt.fill_between(
            subset['created_at'],
            forecast_mean[0] - 2 * forecast_scale[0],
            forecast_mean[0] + 2 * forecast_scale[0],
            color='r', alpha=0.2, label='95% CI'
        )
        plt.xticks(rotation=45)
        plt.title(f'Anomaly Detection for {country} / {platform} / {channel}')
        plt.tight_layout()

        # Save plot
        plot_path = os.path.join(model_output.path, f"diag_{country}_{platform}_{channel}.png")
        plt.savefig(plot_path)
        plt.close()

        # Save model
        model_dict = {
            'model': model,
            'variational_posteriors': variational_posteriors,
            'forecast_mean': forecast_mean,
            'forecast_scale': forecast_scale
        }

        models[(country, platform, channel)] = model_dict

    # Save models
    import pickle
    with open(os.path.join(model_output.path, 'tfp_anomaly_models.pkl'), 'wb') as f:
        pickle.dump(models, f)

    # Create model info file
    with open(os.path.join(model_output.path, 'model.yaml'), 'w') as f:
        f.write(f"""
        name: anomaly-detection-model
        description: TensorFlow Probability STS anomaly detection model
        platform: tensorflow
        version: 1
        """)

    # Add to model registry metadata
    model_output.metadata["framework"] = "tensorflow-probability"
    model_output.metadata["metrics"] = {"groups_modeled": len(models)}