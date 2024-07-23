from google.cloud import bigquery
from google.oauth2 import service_account
import logging

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Configure BigQuery credentials
credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")
project_id = "techlistme"
raw_dataset_id = "raw_data"
extracted_dataset_id = "extracted_data"
raw_table_id = "jobs"
extracted_table_id = "jobs"

client = bigquery.Client(credentials=credentials, project=project_id)


def deduplicate_and_clean():
    temp_table_id = f"{project_id}.{raw_dataset_id}.jobs_deduplicated"

    clean_query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    WITH ranked_jobs AS (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY CASE 
                 WHEN description IS NULL THEN CONCAT(job_id, '_null')
                 ELSE description 
               END
               ORDER BY created_on DESC
             ) AS row_num
      FROM `{project_id}.{raw_dataset_id}.{raw_table_id}`
    )
    SELECT j.* EXCEPT(row_num)
    FROM ranked_jobs j
    LEFT JOIN `{project_id}.{extracted_dataset_id}.{extracted_table_id}` e
      ON j.job_id = e.job_id
    WHERE j.row_num = 1 AND e.job_id IS NULL;
    """

    # Execute the cleaning query
    job = client.query(clean_query)
    job.result()  # Wait for the query to complete

    # Get the number of rows in the original and cleaned tables
    original_count_query = (
        f"SELECT COUNT(*) as count FROM `{project_id}.{raw_dataset_id}.{raw_table_id}`"
    )
    original_count_job = client.query(original_count_query)
    original_count = next(original_count_job.result())[0]

    cleaned_count_query = f"SELECT COUNT(*) as count FROM `{temp_table_id}`"
    cleaned_count_job = client.query(cleaned_count_query)
    cleaned_count = next(cleaned_count_job.result())[0]

    # Replace the original table with the cleaned table
    replace_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{raw_dataset_id}.{raw_table_id}` AS
    SELECT * FROM `{temp_table_id}`;
    """
    job = client.query(replace_query)
    job.result()  # Wait for the query to complete

    # Drop the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info(
        f"Raw data deduplication complete. Rows before: {original_count}, Rows after: {cleaned_count}"
    )


def deduplicate_extracted_data():
    temp_table_id = f"{project_id}.{extracted_dataset_id}.jobs_deduplicated"

    clean_query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    WITH ranked_jobs AS (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY job_id
               ORDER BY created_on DESC
             ) AS job_id_rank,
             ROW_NUMBER() OVER (
               PARTITION BY description
               ORDER BY created_on DESC
             ) AS description_rank
      FROM `{project_id}.{extracted_dataset_id}.{extracted_table_id}`
    )
    SELECT * EXCEPT(job_id_rank, description_rank)
    FROM ranked_jobs
    WHERE job_id_rank = 1 AND description_rank = 1;
    """

    # Execute the cleaning query
    job = client.query(clean_query)
    job.result()  # Wait for the query to complete

    # Get the number of rows in the original and cleaned tables
    original_count_query = f"SELECT COUNT(*) as count FROM `{project_id}.{extracted_dataset_id}.{extracted_table_id}`"
    original_count_job = client.query(original_count_query)
    original_count = next(original_count_job.result())[0]

    cleaned_count_query = f"SELECT COUNT(*) as count FROM `{temp_table_id}`"
    cleaned_count_job = client.query(cleaned_count_query)
    cleaned_count = next(cleaned_count_job.result())[0]

    # Replace the original table with the cleaned table
    replace_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{extracted_dataset_id}.{extracted_table_id}` AS
    SELECT * FROM `{temp_table_id}`;
    """
    job = client.query(replace_query)
    job.result()  # Wait for the query to complete

    # Drop the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info(
        f"Extracted data deduplication complete. Rows before: {original_count}, Rows after: {cleaned_count}"
    )


def remove_processed_jobs():
    temp_table_id = f"{project_id}.{raw_dataset_id}.jobs_unprocessed"

    cleanup_query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    SELECT r.*
    FROM `{project_id}.{raw_dataset_id}.{raw_table_id}` r
    LEFT JOIN `{project_id}.{extracted_dataset_id}.{extracted_table_id}` e
    ON r.job_id = e.job_id
    WHERE e.job_id IS NULL;
    """

    # Execute the cleanup query
    job = client.query(cleanup_query)
    job.result()  # Wait for the query to complete

    # Get the number of rows in the original and cleaned tables
    original_count_query = (
        f"SELECT COUNT(*) as count FROM `{project_id}.{raw_dataset_id}.{raw_table_id}`"
    )
    original_count_job = client.query(original_count_query)
    original_count = next(original_count_job.result())[0]

    cleaned_count_query = f"SELECT COUNT(*) as count FROM `{temp_table_id}`"
    cleaned_count_job = client.query(cleaned_count_query)
    cleaned_count = next(cleaned_count_job.result())[0]

    # Replace the original table with the cleaned table
    replace_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{raw_dataset_id}.{raw_table_id}` AS
    SELECT * FROM `{temp_table_id}`;
    """
    job = client.query(replace_query)
    job.result()  # Wait for the query to complete

    # Drop the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

    removed_count = original_count - cleaned_count
    logging.info(
        f"Processed job removal complete. Rows before: {original_count}, Rows after: {cleaned_count}, Removed: {removed_count}"
    )


if __name__ == "__main__":
    deduplicate_and_clean()
    deduplicate_extracted_data()
    remove_processed_jobs()
