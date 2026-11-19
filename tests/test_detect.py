import shutil

import joblib
import pandas as pd
import pytest

from components.detect_anomalies import detect_anomalies


@pytest.fixture
def fake_gcp(monkeypatch, tmp_path, fitted_models):
    bundle_path = tmp_path / "saved_model.joblib"
    joblib.dump({"metadata": {}, "models": fitted_models}, bundle_path)
    captured = {"bundle_path": bundle_path}

    class FakeBlob:
        def __init__(self, name):
            self.name = name

        def download_to_filename(self, path):
            captured["blob_name"] = self.name
            shutil.copyfile(bundle_path, path)

    class FakeBucket:
        def blob(self, name):
            return FakeBlob(name)

    class FakeStorageClient:
        def bucket(self, name):
            captured["model_bucket"] = name
            return FakeBucket()

    class FakeJob:
        def result(self):
            return None

    class FakeBigQueryClient:
        def __init__(self, project=None, location=None):
            captured["project"] = project
            captured["location"] = location

        def load_table_from_json(self, data, table_id, job_config=None):
            captured["rows"] = data
            captured["table_id"] = table_id
            captured["job_config"] = job_config
            return FakeJob()

    import google.cloud.bigquery as bigquery
    import google.cloud.storage as storage

    monkeypatch.setattr(storage, "Client", lambda: FakeStorageClient())
    monkeypatch.setattr(bigquery, "Client", FakeBigQueryClient)
    return captured


def _with_unseen_combination(df):
    unseen = df.iloc[[0]].copy()
    unseen["country_code"] = "es"
    unseen["new_users"] = 150
    return pd.concat([df, unseen], ignore_index=True)


def test_detect_labels_rows_with_the_matching_combination(
    tmp_path, artifact_factory, prepared_df, fake_gcp
):
    data_path = tmp_path / "prepared.parquet"
    _with_unseen_combination(prepared_df).to_parquet(data_path)

    detect_anomalies.python_func(
        project_id="test-project",
        location="us-central1",
        model_uri="gs://anomaly-detection-dev/pipeline_root/model",
        dataset_name="test_dataset",
        output_table_name="predictions_test",
        infer_data=artifact_factory(data_path),
        raw_infer_data=artifact_factory(data_path),
    )

    rows = fake_gcp["rows"]
    spike = [row for row in rows if row["new_users"] == 100000]
    assert len(spike) == 1
    assert spike[0]["prediction"] == "outlier"

    unseen_rows = [row for row in rows if row["country_code"] == "es"]
    assert len(unseen_rows) == 1
    assert unseen_rows[0]["prediction"] == "no_model"

    labels = {row["prediction"] for row in rows}
    assert "correct" in labels
    assert fake_gcp["table_id"] == "test_dataset.predictions_test"
    assert fake_gcp["project"] == "test-project"
    assert fake_gcp["model_bucket"] == "anomaly-detection-dev"
    assert fake_gcp["blob_name"] == "pipeline_root/model/model.joblib"
    assert all(len(row["created_at"]) == 10 for row in rows)


def test_detect_appends_to_bigquery(tmp_path, artifact_factory, prepared_df, fake_gcp):
    data_path = tmp_path / "prepared.parquet"
    prepared_df.to_parquet(data_path)

    detect_anomalies.python_func(
        project_id="test-project",
        location="us-central1",
        model_uri="gs://anomaly-detection-dev/pipeline_root/model",
        infer_data=artifact_factory(data_path),
        raw_infer_data=artifact_factory(data_path),
    )

    import google.cloud.bigquery as bigquery

    assert fake_gcp["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert len(fake_gcp["rows"]) == len(prepared_df)
