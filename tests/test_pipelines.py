from kfp import compiler

from inference_pipeline import anomaly_detection_inference
from training_pipeline import anomaly_detection_pipeline


def test_training_pipeline_compiles_offline(tmp_path):
    package_path = tmp_path / "training_pipeline.json"
    compiler.Compiler().compile(
        pipeline_func=anomaly_detection_pipeline,
        package_path=str(package_path),
    )

    spec = package_path.read_text()
    assert "extract-data" in spec
    assert "preprocess-data" in spec
    assert "train-ocsvm-model" in spec


def test_inference_pipeline_compiles_offline(tmp_path):
    package_path = tmp_path / "inference_pipeline.json"
    compiler.Compiler().compile(
        pipeline_func=anomaly_detection_inference,
        package_path=str(package_path),
    )

    spec = package_path.read_text()
    assert "extract-data" in spec
    assert "preprocess-data" in spec
    assert "detect-anomalies" in spec
    assert "output_table_name" in spec
