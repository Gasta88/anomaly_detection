from google.cloud import bigquery
import argparse
import random
from datetime import datetime, timedelta

# Define the table name and dataset ID
TABLE_NAME = 'new_users_metrics'
DATASET_ID = 'anomaly_detection'

def generate_random_data(num_days=100):
    data = []
    start_date = datetime.now()
    for day in range(num_days):
        date = start_date + timedelta(days=day)
        for country_code in ["IT", "FR", "DE"]:
            if random.random() < 0.05:
                # Skip 5% of the data
                continue
            for platform in ["ios", "android", "web"]:
                for channel in ["organic", "paid", "referral"]:
                    data.append({
                        "created_at": date.strftime("%Y-%m-%d"),
                        "country_code": country_code,
                        "platform": platform,
                        "channel": channel,
                        "new_users": random.randint(1, 10000)
                    })
    return data

def insert_data(data):
    client = bigquery.Client()
    table_id = f"{DATASET_ID}.{TABLE_NAME}"
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("country_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("channel", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("new_users", bigquery.enums.SqlTypeNames.INTEGER),
    ])
    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()

def truncate_data():
    client = bigquery.Client()
    table_id = f"{DATASET_ID}.{TABLE_NAME}"
    query = f"TRUNCATE TABLE `{table_id}`"
    client.query(query).result()
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['insert', 'truncate'])
    args = parser.parse_args()

    if args.action == "insert":
        insert_data(generate_random_data())
    elif args.action == "truncate":
        truncate_data()
if __name__ == "__main__":
    main()