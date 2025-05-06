from google.cloud import aiplatform
from kfp.dsl import  pipeline
from kfp import compiler
from components.utils import upload_to_gcs
import os
from components.inference import detect_anomalies, save_anomalies_to_bq
from components.utils import upload_to_gcs, BUCKET_NAME

@pipeline(
        name="anomaly-detection-inference-pipeline",
        service_account="vertexai@eighth-duality-457819-r4.iam.gserviceaccount.com")
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

# Compile and run the pipeline
pipeline_file = "anomaly_detection_inference_pipeline.json"
compiler.Compiler().compile(
    pipeline_func=anomaly_detection_inference,
    package_path=pipeline_file
)

# Upload the pipeline file to GCS
upload_to_gcs(
    bucket_name=BUCKET_NAME,
    source_file_name=pipeline_file,
    destination_blob_name=f"pipeline_root/{pipeline_file}"
)

# Deploy the pipeline
# aiplatform.init(project=PROJECT_ID, location=REGION)
# pipeline_job = aiplatform.PipelineJob(
#     display_name="anomaly-detection-inference",
#     template_path=pipeline_file,
#     pipeline_root=f"gs://{BUCKET_NAME}/pipeline_root",
#     credentials=CREDENTIALS,
#     parameter_values={
#         "project_id": PROJECT_ID,
#         "bq_source_table": "anomaly_detection.new_users_metrics",
#         "bq_destination_table": "anomaly_detection.new_users_metrics_preds",
#         "model_name": "anomaly-detection-model"
#     }
# )

# pipeline_job.submit()
os.remove(pipeline_file)