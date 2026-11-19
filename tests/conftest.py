from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM

KEY_COLUMNS = ["country_code", "platform", "channel"]
FEATURE_COLUMNS = ["new_users", "day_of_week", "day_of_month", "month", "quarter"]
COMBINATIONS = [
    (country, platform, channel)
    for country in ["it", "fr"]
    for platform in ["ios", "android"]
    for channel in ["organic", "paid"]
]
SPIKE_COMBINATION = ("it", "ios", "organic")
SPIKE_DAY = 40


def calendar_features(df):
    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["day_of_week"] = df["created_at"].dt.dayofweek
    df["day_of_month"] = df["created_at"].dt.day
    df["month"] = df["created_at"].dt.month
    df["quarter"] = df["created_at"].dt.quarter
    return df.reset_index(drop=True)


@pytest.fixture
def artifact_factory():
    return lambda path: SimpleNamespace(path=str(path))


@pytest.fixture
def raw_df():
    rng = np.random.default_rng(42)
    start = pd.Timestamp("2025-01-01")
    rows = []
    for day in range(60):
        created_at = start + pd.Timedelta(days=day)
        for country, platform, channel in COMBINATIONS:
            new_users = int(rng.integers(100, 200))
            if (country, platform, channel) == SPIKE_COMBINATION and day == SPIKE_DAY:
                new_users = 100000
            rows.append(
                {
                    "created_at": created_at,
                    "country_code": country,
                    "platform": platform,
                    "channel": channel,
                    "new_users": new_users,
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def prepared_df(raw_df):
    return calendar_features(raw_df)


@pytest.fixture
def fitted_models(prepared_df):
    models = {}
    for key, group in prepared_df.groupby(KEY_COLUMNS, sort=True):
        pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("ocsvm", OneClassSVM(kernel="rbf", gamma=0.1, nu=0.1)),
            ]
        )
        pipeline.fit(group[FEATURE_COLUMNS].astype(float))
        models["|".join(str(part) for part in key)] = pipeline
    return models
