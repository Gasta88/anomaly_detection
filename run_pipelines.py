import argparse
from components.utils import PROJECT_ID, REGION, BUCKET_NAME, CREDENTIALS, SERVICE_ACCOUNT, MODEL_NAME, BQ_DATASET_NAME
from google.cloud import aiplatform



def run_training_pipeline():
    aiplatform.init(project=PROJECT_ID, location=REGION)
    pipeline_job = aiplatform.PipelineJob(
        display_name="anomaly-detection-training",
        template_path=f"gs://{BUCKET_NAME}/pipeline_root/anomaly_detection_pipeline.json",
        pipeline_root=f"gs://{BUCKET_NAME}/pipeline_root",
        credentials=CREDENTIALS,
        parameter_values={
            "project_id": PROJECT_ID,
            "bq_table": "{BQ_DATASET_NAME}.new_users_metrics",
            "bucket_name": BUCKET_NAME
        },
        enable_caching=False
    )
    pipeline_job.submit()

def run_inference_pipeline():
    aiplatform.init(project=PROJECT_ID, location=REGION)
    MODEL_URI = [
        m.resource_name
        for m in aiplatform.Model.list(order_by="create_time desc")
        if MODEL_NAME in m.display_name
    ][0]
    pipeline_job = aiplatform.PipelineJob(
        display_name="anomaly-detection-inference",
        template_path=f"gs://{BUCKET_NAME}/pipeline_root/anomaly_detection_inference_pipeline.json",
        pipeline_root=f"gs://{BUCKET_NAME}/pipeline_root",
        credentials=CREDENTIALS,
        parameter_values={
            "project_id": PROJECT_ID,
            "location": REGION,
            "bigquery_source_input_uri": f"bq://{PROJECT_ID}.{BQ_DATASET_NAME}.new_users_metrics",
            "bigquery_destination_output_uri": f"bq://{PROJECT_ID}.{BQ_DATASET_NAME}",
            "model_uri": MODEL_URI,
            "service_account": SERVICE_ACCOUNT
        },
        enable_caching=False
    )
    pipeline_job.submit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['train', 'infer'])
    args = parser.parse_args()

    if args.action == "train":
        run_training_pipeline()
    elif args.action == "infer":
        run_inference_pipeline()
if __name__ == "__main__":
    main()