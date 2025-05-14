from google.cloud import aiplatform
from kfp.dsl import  pipeline
from kfp import compiler
from components.utils import upload_to_gcs
import os
from components.inference.detect_anomalies import detect_anomalies
from components.inference.get_model import get_model
from components.utils import upload_to_gcs, BUCKET_NAME


@pipeline(name="anomaly-detection-inference-pipeline")
def anomaly_detection_inference(
    project_id: str,
    location: str,
    bq_source_table: str,
    bq_destination_table: str,
    model_name: str,
    service_account: str
):
    # Get the latest model
    get_model_op = get_model(
        project_id=project_id, location=location, model_name=model_name
        )

    # Detect anomalies
    detect_op = detect_anomalies(
        project_id=project_id,
        location=location,
        src_bq_table=bq_source_table,
        dest_bq_table=bq_destination_table,
        model=get_model_op.outputs["model"],
        service_account=service_account
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

os.remove(pipeline_file)