from kfp.dsl import  pipeline
from kfp import compiler
import os
from components.extract_data import extract_data
from components.preprocess_data import preprocess_data
from components.detect_anomalies import detect_anomalies
from components.utils import upload_to_gcs, BUCKET_NAME



@pipeline(name="anomaly-detection-inference-pipeline")
def anomaly_detection_inference(
    project_id: str,
    location: str,
    query: str,
    model_uri: str,
    bucket_name: str
):
    extract_op = extract_data(project_id=project_id, query=query)

    preprocess_op = preprocess_data(
        input_data=extract_op.outputs["output_data"], mode="infer"
    )

    detect_op = detect_anomalies(
        project_id=project_id,
        location=location,
        bucket_name=bucket_name,
        raw_infer_data = extract_op.outputs["output_data"],
        infer_data=preprocess_op.outputs["output_data"],
        model_uri=model_uri
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