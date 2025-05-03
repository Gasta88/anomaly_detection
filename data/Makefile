DATASET_NAME = anomaly_detection
TABLE_NAME = new_users_metrics
PYTHON_SCRIPT = manage_data.py
PROJECT_ID = eighth-duality-457819-r4

# Create an empty BigQuery dataset and table
create_dataset:
	@echo "Creating dataset $(DATASET_NAME) and table $(TABLE_NAME)"
	@bq mk --location=us-central1 --dataset --default_table_expiration=86400 $(PROJECT_ID):$(DATASET_NAME)
	@bq mk --table $(PROJECT_ID):$(DATASET_NAME).$(TABLE_NAME) schema.json

# Remove the given BigQuery dataset and table
clean_dataset:
	@echo "Removing dataset $(DATASET_NAME) and table $(TABLE_NAME)"
	@bq rm -r -f -d $(PROJECT_ID):$(DATASET_NAME)

# Insert data into the BigQuery table
insert_data:
	@echo "Inserting data into table $(TABLE_NAME)"
	@python $(PYTHON_SCRIPT) insert

# Truncate the BigQuery table
purge_data:
	@echo "Purging data from table $(TABLE_NAME)"
	@python $(PYTHON_SCRIPT) truncate

# Default target
create: create_dataset insert_data
destroy: purge_data clean_dataset

.PHONY: create_dataset clean_dataset insert_data purge_data create destroy