import joblib
import numpy as np
import pytest

from components.train_ocsvm_model import train_ocsvm_model

COMBINATIONS = [(country, platform, channel) for country in ["it", "fr"] for platform in ["ios", "android"] for channel in ["organic", "paid"]]
FEATURE_COLUMNS = ["new_users", "day_of_week", "day_of_month", "month", "quarter"]


@pytest.fixture
def fake_gcp(monkeypatch):
    captured = {}

    class FakeBlob:
        def __init__(self, name):
            self.name = name

        def upload_from_filename(self, path):
            captured["blob_name"] = self.name
            captured["uploaded_file"] = path

    class FakeBucket:
        def blob(self, name):
            return FakeBlob(name)

    class FakeStorageClient:
        def get_bucket(self, name):
            captured["bucket"] = name
            return FakeBucket()

    class FakeModel:
        display_name = "ocsvm_model"

    class FakeModelClass:
        @staticmethod
        def upload(**kwargs):
            captured["upload_kwargs"] = kwargs
            return FakeModel()

    import google.cloud.aiplatform as aiplatform
    import google.cloud.storage as storage

    monkeypatch.setattr(storage, "Client", lambda: FakeStorageClient())
    monkeypatch.setattr(aiplatform, "init", lambda **kwargs: captured.setdefault("init", kwargs))
    monkeypatch.setattr(aiplatform, "Model", FakeModelClass)
    return captured


def test_training_fits_one_model_per_combination(tmp_path, artifact_factory, prepared_df, fake_gcp):
    data_path = tmp_path / "prepared.parquet"
    prepared_df.to_parquet(data_path)

    train_ocsvm_model.python_func(
        project_id="test-project",
        location="us-central1",
        bucket_name="anomaly-detection-dev",
        training_data=artifact_factory(data_path),
        model_output=artifact_factory("/gcs/anomaly-detection-dev/pipeline_root/model"),
    )

    bundle = joblib.load(fake_gcp["uploaded_file"])
    expected = {"|".join(combination) for combination in COMBINATIONS}
    assert set(bundle["models"]) == expected
    assert bundle["metadata"]["features"] == FEATURE_COLUMNS
    assert bundle["metadata"]["skipped_combinations"] == {}
    assert fake_gcp["blob_name"].endswith("pipeline_root/model/model.joblib")
    assert fake_gcp["upload_kwargs"]["display_name"] == "ocsvm_model"


def test_training_flags_an_injected_spike(tmp_path, artifact_factory, prepared_df, fake_gcp):
    data_path = tmp_path / "prepared.parquet"
    prepared_df.to_parquet(data_path)

    train_ocsvm_model.python_func(
        project_id="test-project",
        location="us-central1",
        bucket_name="anomaly-detection-dev",
        training_data=artifact_factory(data_path),
        model_output=artifact_factory("/gcs/anomaly-detection-dev/pipeline_root/model"),
    )

    bundle = joblib.load(fake_gcp["uploaded_file"])
    spike = prepared_df[prepared_df["new_users"] == 100000]
    assert len(spike) == 1
    model = bundle["models"]["it|ios|organic"]
    assert model.predict(spike[FEATURE_COLUMNS].astype(float))[0] == -1

    normal = prepared_df[prepared_df["new_users"] < 100000]
    normal_model = bundle["models"]["fr|android|paid"]
    normal_subset = normal[
        (normal["country_code"] == "fr")
        & (normal["platform"] == "android")
        & (normal["channel"] == "paid")
    ]
    predictions = normal_model.predict(normal_subset[FEATURE_COLUMNS].astype(float))
    assert (predictions == 1).mean() > 0.8
    assert np.unique(predictions).tolist() == [-1, 1]
