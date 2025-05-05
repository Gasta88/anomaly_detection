from google.cloud import aiplatform
from kfp.dsl import pipeline, PipelineConfig
from kfp import compiler
import os
from components.data_ingestion.extract_data import extract_data
from components.data_processing.preprocess_data import preprocess_data
from components.model_training.train_tfp_model import train_tfp_model
from components.utils import upload_to_gcs

ENV = os.environ.get("ENV", "dev")
PROJECT_ID = "eighth-duality-457819-r4"
REGION = "us-central1"
BUCKET_NAME = f"bondola-ai-anomaly-detection-{ENV}"
pipeline_config = PipelineConfig(
    service_account="vertexai@eighth-duality-457819-r4.iam.gserviceaccount.com"
)
@pipeline(
        name="anomaly-detection-training-pipeline"
        )
def anomaly_detection_pipeline(
    project_id: str,
    bq_table: str,
    bucket_name: str
):
    extract_op = extract_data(project_id=project_id, bq_table=bq_table)

    preprocess_op = preprocess_data(
        data=extract_op.outputs["output_data"]
    )

    train_op = train_tfp_model(
        training_data=preprocess_op.outputs["output_training_data"],
        metadata=preprocess_op.outputs["output_metadata"]
    )

# Compile and run the pipeline
pipeline_file = "anomaly_detection_pipeline.json"
compiler.Compiler().compile(
    pipeline_func=anomaly_detection_pipeline,
    package_path=pipeline_file,
    pipeline_config=pipeline_config
)

# Upload the pipeline file to GCS
upload_to_gcs(
    bucket_name=BUCKET_NAME,
    source_file_name=pipeline_file,
    destination_blob_name=f"pipeline_root/{pipeline_file}"
)

# Deploy the pipeline
aiplatform.init(project=PROJECT_ID, location=REGION)
pipeline_job = aiplatform.PipelineJob(
    display_name="anomaly-detection-training",
    template_path=pipeline_file,
    pipeline_root=f"gs://{BUCKET_NAME}/pipeline_root",
    parameter_values={
        "project_id": PROJECT_ID,
        "bq_table": "anomaly_detection.new_users_metrics",
        "bucket_name": BUCKET_NAME
    }
)

os.remove(pipeline_file)
# pipeline_job.run()