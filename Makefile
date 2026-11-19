-include .env
export

ENV ?= dev
PROJECT_ID ?=
REGION ?= us-central1
DATASET_NAME ?= anomaly_detection
INPUT_TABLE_NAME ?= new_users_metrics
OUTPUT_TABLE_NAME ?= predictions
BUCKET_NAME ?= anomaly-detection-$(ENV)
PYTHON_SCRIPT = data/manage_data.py

# Create an empty BigQuery dataset and table
create_dataset: check_env
	@echo "Creating dataset $(DATASET_NAME) and tables $(INPUT_TABLE_NAME), $(OUTPUT_TABLE_NAME)"
	@bq mk --location=$(REGION) --dataset --default_table_expiration=86400 $(PROJECT_ID):$(DATASET_NAME)
	@bq mk --table $(PROJECT_ID):$(DATASET_NAME).$(INPUT_TABLE_NAME) data/input_table_schema.json
	@bq mk --table $(PROJECT_ID):$(DATASET_NAME).$(OUTPUT_TABLE_NAME) data/output_table_schema.json

# Remove the given BigQuery dataset and table
delete_dataset: check_env
	@echo "Removing dataset $(DATASET_NAME) and tables"
	@bq rm -r -f -d $(PROJECT_ID):$(DATASET_NAME)

# Insert data into the BigQuery table
insert_data: check_env
	@echo "Inserting data into table $(INPUT_TABLE_NAME)"
	@python $(PYTHON_SCRIPT) insert

# Truncate the BigQuery table
truncate_data: check_env
	@echo "Purging data from tables"
	@python $(PYTHON_SCRIPT) truncate

# Create GCS bucket for artefact storage
create_bucket: check_env
	@echo "Creating bucket $(BUCKET_NAME)"
	@gcloud storage buckets create gs://$(BUCKET_NAME) --location $(REGION)

# Delete GCS bucket for artefact storage
delete_bucket: check_env
	@echo "Deleting bucket $(BUCKET_NAME)"
	@gcloud storage rm --recursive gs://$(BUCKET_NAME)

# Delete models from Model Registry
delete_models: check_env
	@echo "Deleting models from Model Registry"
	@gcloud ai models delete $$(gcloud ai models list --format='get(name)' --region=$(REGION)) --region=$(REGION) --quiet

# Compile and upload pipelines
create_pipelines: check_env
	@echo "Creating pipelines files"
	@python training_pipeline.py
	@python inference_pipeline.py

# Run training pipeline
train: check_env
	@echo "Running training pipeline"
	@python run_pipelines.py train

# Run inference pipeline
infer: check_env
	@echo "Running inference pipeline"
	@python run_pipelines.py infer

# Install runtime and development dependencies
install:
	@python -m pip install -r requirements-dev.txt

# Static checks
lint:
	@ruff check .

# Contract tests
test:
	@python -m pytest

check_env:
	@test -n "$(PROJECT_ID)" || (echo "PROJECT_ID is not set. Copy .env.example to .env and fill it in." && exit 1)

# Default target
create: create_dataset insert_data create_bucket create_pipelines
destroy: truncate_data delete_dataset delete_bucket delete_models

.PHONY: create_dataset delete_dataset insert_data truncate_data create_bucket \
		delete_bucket delete_models create_pipelines train infer create destroy \
		install lint test check_env
