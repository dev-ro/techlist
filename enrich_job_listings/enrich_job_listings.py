import requests
import time
import logging
from bs4 import BeautifulSoup
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery

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

def update_job_descriptions(job_data):
    df = pd.DataFrame(job_data)
    temp_table_id = f"{project_id}.raw_data.temp_table"
    table_id_full = f"{project_id}.raw_data.{table_id}"
    
    # Load the data into a temporary table
    job_data_df = pd.DataFrame(job_data)
    pandas_gbq.to_gbq(job_data_df, temp_table_id, project_id, if_exists='replace', credentials=credentials)

    # Perform the MERGE operation
    merge_query = f"""
    MERGE `{table_id_full}` T
    USING (
      SELECT job_id, description, created_on FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY created_on DESC) AS rnum
        FROM `{temp_table_id}`
      ) WHERE rnum = 1
    ) S
    ON T.job_id = S.job_id
    WHEN MATCHED THEN
      UPDATE SET T.description = S.description, T.created_on = S.created_on
    WHEN NOT MATCHED THEN
      INSERT (job_id, description, created_on) VALUES (S.job_id, S.description, S.created_on)
    """

    job = client.query(merge_query)
    job.result()  # Wait for the query to complete

    # Clean up the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

    logging.info(f"Updated {len(job_data)} job descriptions in BigQuery")

def job_detail_request(job_id, retry_count=0):
    url = f"https://www.linkedin.com/jobs/view/{job_id}"
    try:
        start_time = time.time()
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url=url, headers=headers, timeout=5)
        elapsed = time.time() - start_time
        logging.info(
            f"job_id: {job_id} status_code: {response.status_code} elapsed: {elapsed:.2f}"
        )

        if response.status_code == 200:
            # Extract description
            soup = BeautifulSoup(response.content, "html.parser")
            description = soup.find(attrs={"class": "show-more-less-html__markup"})
            description = (
                description.getText(separator="\n", strip=True) if description else ""
            )
            return {"job_id": job_id, "description": description, "created_on": time.time()}

        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 2))
            if retry_count < 10:
                sleep_time = retry_after + retry_count**2  
                logging.warning(
                    f"Rate limited. Retrying after {sleep_time} seconds..."
                )
                
                time.sleep(sleep_time)
                return job_detail_request(job_id, retry_count + 1)
            else:
                logging.error(
                    f"Failed to retrieve job_id: {job_id} after {retry_count} retries"
                )

        if response.status_code in [400, 404]:
            logging.warning(f"Job ID: {job_id} may be invalid or deleted.")
            return {"job_id": job_id, "description": "", "created_on": time.time()}

    except Exception as e:
        logging.error(f"Error in job_detail_request: {e}")
        return {"job_id": job_id, "description": "", "created_on": time.time()}

def enrich_jobs():
    # Load jobs without descriptions from BigQuery
    query = f"""
    SELECT job_id FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE description IS NULL OR description = ""
    """
    jobs_df = client.query(query).to_dataframe()
    job_ids = jobs_df["job_id"].tolist()

    batch_size = 100
    job_data = []

    for i, job_id in enumerate(job_ids):
        job_description = job_detail_request(job_id)
        job_data.append(job_description)

        # Upload to BigQuery every batch_size jobs
        if (i + 1) % batch_size == 0:
            update_job_descriptions(job_data)
            job_data = []

    # Upload remaining jobs
    if job_data:
        update_job_descriptions(job_data)

if __name__ == "__main__":
    enrich_jobs()
