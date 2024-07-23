import requests
import time
import logging
from bs4 import BeautifulSoup
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Configure BigQuery credentials
credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")
project_id = "techlistme"
dataset_id = "raw_data"
table_id = "jobs"

client = bigquery.Client(credentials=credentials, project=project_id)
user_agent = UserAgent()

def update_job_descriptions(job_data):
    temp_table_id = f"{project_id}.{dataset_id}.temp_table"
    table_id_full = f"{project_id}.{dataset_id}.{table_id}"
    
    # Load the data into a temporary table
    job_data_df = pd.DataFrame(job_data)
    pandas_gbq.to_gbq(job_data_df, temp_table_id, project_id, if_exists='replace', credentials=credentials)

    # Perform the MERGE operation
    merge_query = f"""
    MERGE `{table_id_full}` T
    USING (
      SELECT job_id, description, created_on, url FROM `{temp_table_id}`
    ) S
    ON T.job_id = S.job_id
    WHEN MATCHED THEN
      UPDATE SET T.description = S.description, T.created_on = S.created_on, T.url = S.url
    WHEN NOT MATCHED THEN
      INSERT (job_id, description, created_on, url) VALUES (S.job_id, S.description, S.created_on, S.url)
    """

    client.query(merge_query).result()
    client.delete_table(temp_table_id, not_found_ok=True)
    logging.info(f"Updated {len(job_data)} job descriptions in BigQuery")

def job_detail_request(job_id, max_retries=8, base_delay=2):
    url = f"https://www.linkedin.com/jobs/view/{job_id}"
    for retry in range(max_retries):
        try:
            headers = {"User-Agent": user_agent.random}
            response = requests.get(url=url, headers=headers, timeout=5)
            logging.info(f"job_id: {job_id} status_code: {response.status_code}")

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                description = soup.find(attrs={"class": "show-more-less-html__markup"})
                description = description.getText(separator="\n", strip=True) if description else ""
                return {"job_id": job_id, "description": description, "created_on": time.time(), "url": url}

            if response.status_code in [400, 404]:
                logging.warning(f"Job ID: {job_id} may be invalid or deleted.")
                return {"job_id": job_id, "description": "", "created_on": time.time(), "url": url}

            if response.status_code == 429:
                sleep_time = base_delay * (2 ** retry)
                logging.warning(f"Rate limited. Retrying after {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue

        except Exception as e:
            logging.error(f"Error in job_detail_request for job_id {job_id}: {e}")

    logging.error(f"Failed to retrieve job_id: {job_id} after {max_retries} retries")
    return {"job_id": job_id, "description": "", "created_on": time.time(), "url": url}

def enrich_jobs(batch_size=100, max_workers=2):
    query = f"""
    SELECT job_id FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE description IS NULL OR description = ""
    """
    job_ids = client.query(query).to_dataframe()["job_id"].tolist()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_job = {executor.submit(job_detail_request, job_id): job_id for job_id in job_ids}
        job_data = []

        for future in as_completed(future_to_job):
            job_data.append(future.result())

            if len(job_data) >= batch_size:
                update_job_descriptions(job_data)
                job_data = []

        if job_data:
            update_job_descriptions(job_data)

if __name__ == "__main__":
    enrich_jobs()