DATASET_NAME = anomaly_detection
REGION = us-central1
INPUT_TABLE_NAME = new_users_metrics
OUTPUT_TABLE_NAME = predictions
PYTHON_SCRIPT = data/manage_data.py
PROJECT_ID = my-cool-project-457818
BUCKET_NAME = anomaly-detection
ENV = dev

# Create an empty BigQuery dataset and table
create_dataset:
	@echo "Creating dataset $(DATASET_NAME) and tables $(INPUT_TABLE_NAME), $(OUTPUT_TABLE_NAME)"
	@bq mk --location=$(REGION) --dataset --default_table_expiration=86400 $(PROJECT_ID):$(DATASET_NAME)
	@bq mk --table $(PROJECT_ID):$(DATASET_NAME).$(INPUT_TABLE_NAME) data/input_table_schema.json
	@bq mk --table $(PROJECT_ID):$(DATASET_NAME).$(OUTPUT_TABLE_NAME) data/output_table_schema.json

# Remove the given BigQuery dataset and table
delete_dataset:
	@echo "Removing dataset $(DATASET_NAME) and tables"
	@bq rm -r -f -d $(PROJECT_ID):$(DATASET_NAME)

# Insert data into the BigQuery table
insert_data:
	@echo "Inserting data into table $(INPUT_TABLE_NAME)"
	@python $(PYTHON_SCRIPT) insert

# Truncate the BigQuery table
truncate_data:
	@echo "Purging data from tables"
	@python $(PYTHON_SCRIPT) truncate

# Create GCS for artefact storage
create_bucket:
	@echo "Creating bucket $(BUCKET_NAME)-$(ENV)"
	@gcloud storage buckets create gs://$(BUCKET_NAME)-$(ENV) --location $(REGION)

# Delete GCS for artefact storage
delete_bucket:
	@echo "Deleting bucket $(BUCKET_NAME)-$(ENV)"
	@gcloud storage rm --recursive gs://$(BUCKET_NAME)-$(ENV)

# Delete models from Model Registry
delete_models:
	@echo "Deleting models Model Registry-$(ENV)"
	@gcloud ai models delete $$(gcloud ai models list --format='get(name)' --region=$(REGION)) --region=$(REGION) --quiet

# Compile and upload pipelines
create_pipelines:
	@echo "Creating pipelines files"
	@python training_pipeline.py
	@python inference_pipeline.py

# Run training pipeline
train:
	@echo "Running training pipeline"
	@python run_pipelines.py train

#Run inference pipeline
infer:
	@echo "Running inference pipeline"
	@python run_pipelines.py infer

# Default target
create: create_dataset insert_data create_bucket create_pipelines
destroy: truncate_data delete_dataset delete_bucket delete_models

.PHONY: create_dataset delete_dataset insert_data truncate_data create_bucket \
 		delete_bucket delete_models create_pipelines train infer create destroy 