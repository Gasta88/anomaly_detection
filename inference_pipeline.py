import os

from kfp import compiler
from kfp.dsl import pipeline

from components.detect_anomalies import detect_anomalies
from components.extract_data import extract_data
from components.preprocess_data import preprocess_data
from components.utils import BUCKET_NAME, upload_to_gcs


@pipeline(name="anomaly-detection-inference-pipeline")
def anomaly_detection_inference(
    project_id: str,
    location: str,
    query: str,
    model_uri: str,
    bucket_name: str,
    dataset_name: str = "anomaly_detection",
    output_table_name: str = "predictions",
):
    extract_op = extract_data(project_id=project_id, query=query)

    preprocess_op = preprocess_data(input_data=extract_op.outputs["output_data"])

    detect_anomalies(
        project_id=project_id,
        location=location,
        dataset_name=dataset_name,
        output_table_name=output_table_name,
        raw_infer_data=extract_op.outputs["output_data"],
        infer_data=preprocess_op.outputs["output_data"],
        model_uri=model_uri,
    )


def main(pipeline_file="anomaly_detection_inference_pipeline.json"):
    compiler.Compiler().compile(
        pipeline_func=anomaly_detection_inference,
        package_path=pipeline_file,
    )
    upload_to_gcs(
        bucket_name=BUCKET_NAME,
        source_file_name=pipeline_file,
        destination_blob_name=f"pipeline_root/{pipeline_file}",
    )
    os.remove(pipeline_file)


if __name__ == "__main__":
    main()
