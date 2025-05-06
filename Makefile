DATASET_NAME = anomaly_detection
TABLE_NAME = new_users_metrics
PYTHON_SCRIPT = data/manage_data.py
PROJECT_ID = eighth-duality-457819-r4
BUCKET_NAME = bondola-ai-anomaly-detection
ENV = dev

# Create an empty BigQuery dataset and table
create_dataset:
	@echo "Creating dataset $(DATASET_NAME) and table $(TABLE_NAME)"
	@bq mk --location=us-central1 --dataset --default_table_expiration=86400 $(PROJECT_ID):$(DATASET_NAME)
	@bq mk --table $(PROJECT_ID):$(DATASET_NAME).$(TABLE_NAME) data/schema.json

# Remove the given BigQuery dataset and table
delete_dataset:
	@echo "Removing dataset $(DATASET_NAME) and table $(TABLE_NAME)"
	@bq rm -r -f -d $(PROJECT_ID):$(DATASET_NAME)

# Insert data into the BigQuery table
insert_data:
	@echo "Inserting data into table $(TABLE_NAME)"
	@python $(PYTHON_SCRIPT) insert

# Truncate the BigQuery table
truncate_data:
	@echo "Purging data from table $(TABLE_NAME)"
	@python $(PYTHON_SCRIPT) truncate

create_bucket:
	@echo "Creating bucket $(BUCKET_NAME)-$(ENV)"
	@gcloud storage buckets create gs://$(BUCKET_NAME)-$(ENV) --location us-central1

delete_bucket:
	@echo "Deleting bucket $(BUCKET_NAME)-$(ENV)"
	@gcloud storage rm --recursive gs://$(BUCKET_NAME)-$(ENV)

# Default target
create: create_dataset insert_data create_bucket
destroy: truncate_data delete_dataset delete_bucket

.PHONY: create_dataset delete_dataset insert_data truncate_data create_bucket delete_bucket create destroy