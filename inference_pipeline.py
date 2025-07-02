from kfp.dsl import  pipeline
from kfp import compiler,dsl
from components.utils import upload_to_gcs
from google_cloud_pipeline_components.types import artifact_types
import os
from google_cloud_pipeline_components.v1.batch_predict_job import ModelBatchPredictOp
from components.data_processing.preprocess_data_infer import preprocess_data_infer
from components.utils import upload_to_gcs, BUCKET_NAME



@pipeline(name="anomaly-detection-inference-pipeline")
def anomaly_detection_inference(
    project_id: str,
    location: str,
    bigquery_source_input_uri: str,
    bigquery_destination_output_uri: str,
    model_uri: str,
    service_account: str
):
    # Get the latest model
    importer_spec = dsl.importer(
        artifact_uri=model_uri,
        artifact_class=artifact_types.VertexModel,
        reimport=False,
        metadata={"resourceName": model_uri},
    )

    preprocess_op = preprocess_data_infer(
        project_id=project_id, src_bq_table=bigquery_source_input_uri
    ).after(importer_spec)


    detect_op = ModelBatchPredictOp(
        project=project_id,
        location=location,
        model=importer_spec.outputs["artifact"],
        job_display_name='sklearn-bq-batch-predict-job',
        instances_format='bigquery',
        predictions_format='bigquery',
        bigquery_source_input_uri=bigquery_source_input_uri,
        bigquery_destination_output_uri=bigquery_destination_output_uri,
        service_account=service_account,
        machine_type='n1-standard-8',
    ).after(preprocess_op)


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