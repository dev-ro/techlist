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

def check_for_duplicate_descriptions():
    query = f"""
    SELECT job_id, description, COUNT(*) as count
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE description IS NOT NULL AND description != ""
    GROUP BY job_id, description
    HAVING count > 1;
    """
    duplicates = client.query(query).result()
    return len(list(duplicates)) > 0

def clean_duplicate_descriptions():
    temp_table_id = f"{project_id}.{dataset_id}.jobs_clean_desc"

    # Create a temporary table with unique descriptions
    clean_query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    SELECT * FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY job_id, description ORDER BY created_on ASC) AS row_num
      FROM `{project_id}.{dataset_id}.{table_id}`
      WHERE description IS NOT NULL AND description != ""
    )
    WHERE row_num = 1;
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

    logging.info("Duplicate descriptions have been cleaned from BigQuery")

if __name__ == "__main__":
    if check_for_duplicate_descriptions():
        clean_duplicate_descriptions()
    else:
        logging.info("No duplicate descriptions found.")
