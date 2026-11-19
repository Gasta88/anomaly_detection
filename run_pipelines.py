import argparse

from google.cloud import aiplatform

from components.utils import (
    BQ_DATASET_NAME,
    BQ_OUTPUT_TABLE_NAME,
    BUCKET_NAME,
    MODEL_NAME,
    PREDICT_SQL,
    PROJECT_ID,
    REGION,
    TRAIN_SQL,
    get_credentials,
)


def require_project_id():
    if not PROJECT_ID:
        raise SystemExit(
            "PROJECT_ID is not set. Copy .env.example to .env and fill it in, "
            "or export PROJECT_ID before running."
        )


def run_training_pipeline():
    aiplatform.init(project=PROJECT_ID, location=REGION)
    pipeline_job = aiplatform.PipelineJob(
        display_name="anomaly-detection-training",
        template_path=f"gs://{BUCKET_NAME}/pipeline_root/anomaly_detection_pipeline.json",
        pipeline_root=f"gs://{BUCKET_NAME}/pipeline_root",
        credentials=get_credentials(),
        parameter_values={
            "project_id": PROJECT_ID,
            "location": REGION,
            "query": TRAIN_SQL,
            "bucket_name": BUCKET_NAME,
        },
        enable_caching=False,
    )
    return pipeline_job


def run_inference_pipeline():
    aiplatform.init(project=PROJECT_ID, location=REGION)
    model_uris = [
        model.uri
        for model in aiplatform.Model.list(order_by="create_time desc")
        if MODEL_NAME in model.display_name
    ]
    if not model_uris:
        raise SystemExit(f"No model matching '{MODEL_NAME}' found in the Model Registry.")
    pipeline_job = aiplatform.PipelineJob(
        display_name="anomaly-detection-inference",
        template_path=(
            f"gs://{BUCKET_NAME}/pipeline_root/anomaly_detection_inference_pipeline.json"
        ),
        pipeline_root=f"gs://{BUCKET_NAME}/pipeline_root",
        credentials=get_credentials(),
        parameter_values={
            "project_id": PROJECT_ID,
            "location": REGION,
            "query": PREDICT_SQL,
            "model_uri": model_uris[0],
            "bucket_name": BUCKET_NAME,
            "dataset_name": BQ_DATASET_NAME,
            "output_table_name": BQ_OUTPUT_TABLE_NAME,
        },
        enable_caching=True,
    )
    return pipeline_job


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["train", "infer"])
    args = parser.parse_args()

    require_project_id()

    if args.action == "train":
        pipeline_job = run_training_pipeline()
    else:
        pipeline_job = run_inference_pipeline()

    pipeline_job.submit()


if __name__ == "__main__":
    main()
