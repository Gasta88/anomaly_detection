import pandas as pd

from components.preprocess_data import preprocess_data


def test_preprocess_adds_calendar_features(tmp_path, artifact_factory, raw_df):
    input_path = tmp_path / "raw.parquet"
    output_path = tmp_path / "prepared.parquet"
    raw_df.to_parquet(input_path)

    preprocess_data.python_func(
        input_data=artifact_factory(input_path),
        output_data=artifact_factory(output_path),
    )

    result = pd.read_parquet(output_path)
    assert len(result) == len(raw_df)
    for column in ["day_of_week", "day_of_month", "month", "quarter"]:
        assert column in result.columns
    assert result["day_of_week"].between(0, 6).all()
    assert result["month"].between(1, 12).all()


def test_preprocess_preserves_row_order(tmp_path, artifact_factory, raw_df):
    input_path = tmp_path / "raw.parquet"
    output_path = tmp_path / "prepared.parquet"
    raw_df.to_parquet(input_path)

    preprocess_data.python_func(
        input_data=artifact_factory(input_path),
        output_data=artifact_factory(output_path),
    )

    result = pd.read_parquet(output_path)
    assert result["new_users"].tolist() == raw_df["new_users"].tolist()
    assert result["country_code"].tolist() == raw_df["country_code"].tolist()
