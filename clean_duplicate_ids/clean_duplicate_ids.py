from google.oauth2 import service_account
from google.cloud import bigquery
import logging

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

# Configure BigQuery credentials
credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")
project_id = "techlistme"
dataset_id = "raw_data"
table_id = "jobs"

client = bigquery.Client(credentials=credentials, project=project_id)

def check_for_duplicates():
    query = f"""
    SELECT job_id, COUNT(*) as count
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY job_id
    HAVING count > 1;
    """
    duplicates = client.query(query).result()
    return len(list(duplicates)) > 0

def clean_duplicate_ids():
    temp_table_id = f"{project_id}.{dataset_id}.jobs_clean"

    # Create a temporary table with unique job IDs
    clean_query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    SELECT 
        ANY_VALUE(description) as description,
        ANY_VALUE(task_id) as task_id,
        ANY_VALUE(keyword) as keyword,
        ANY_VALUE(location) as location,
        ANY_VALUE(company) as company,
        ANY_VALUE(title) as title,
        ANY_VALUE(url) as url,
        job_id,
        MAX(created_on) as created_on
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY job_id;
    """
    client.query(clean_query).result()

    # Replace the original table with the cleaned table
    replace_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
    SELECT * FROM `{temp_table_id}`;
    """
    client.query(replace_query).result()

    # Optionally, drop the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info("Duplicate job IDs have been cleaned from BigQuery")

if __name__ == "__main__":
    if check_for_duplicates():
        clean_duplicate_ids()
    else:
        logging.info("No duplicate job IDs found.")
