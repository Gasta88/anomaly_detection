import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _requirement_lines():
    text = (REPO_ROOT / "requirements.txt").read_text()
    return [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]


def test_requirements_are_pinned():
    for line in _requirement_lines():
        assert "==" in line, f"Unpinned requirement: {line}"


def test_requirements_have_no_duplicates():
    names = [re.split(r"[=<>!]", line)[0].lower() for line in _requirement_lines()]
    assert len(names) == len(set(names))


def test_requirements_include_runtime_imports():
    names = {re.split(r"[=<>!]", line)[0].lower() for line in _requirement_lines()}
    for package in [
        "google-cloud-aiplatform",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "kfp",
        "pandas",
        "pyarrow",
        "scikit-learn",
        "python-dotenv",
    ]:
        assert package in names
