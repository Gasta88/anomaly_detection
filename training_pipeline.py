from kfp.dsl import pipeline
from kfp import compiler
import os
from components.data_ingestion.extract_data import extract_data
from components.data_processing.preprocess_data_train import preprocess_data_train
from components.model_training.train_ocsvm_model import train_ocsvm_model
from components.utils import upload_to_gcs, BUCKET_NAME

@pipeline(
        name="anomaly-detection-training-pipeline"
        )
def anomaly_detection_pipeline(
    project_id: str,
    bq_table: str,
    bucket_name: str
):
    extract_op = extract_data(project_id=project_id, bq_table=bq_table)

    preprocess_op = preprocess_data_train(
        data=extract_op.outputs["output_data"]
    )

    train_op = train_ocsvm_model(
        project_id=project_id,
        location="us-central1",
        bucket_name=BUCKET_NAME,
        training_data=preprocess_op.outputs["output_training_data"],
        metadata=preprocess_op.outputs["output_metadata"]
    )

# Compile and run the pipeline
pipeline_file = "anomaly_detection_pipeline.json"
compiler.Compiler().compile(
    pipeline_func=anomaly_detection_pipeline,
    package_path=pipeline_file,
)

# Upload the pipeline file to GCS
upload_to_gcs(
    bucket_name=BUCKET_NAME,
    source_file_name=pipeline_file,
    destination_blob_name=f"pipeline_root/{pipeline_file}"
)

os.remove(pipeline_file)