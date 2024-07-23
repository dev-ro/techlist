import requests
import time
import logging
from bs4 import BeautifulSoup
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

# Configure BigQuery credentials
credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")
project_id = "techlistme"
table_id = "raw_data.jobs"

def update_job_descriptions(job_data):
    df = pd.DataFrame(job_data)
    pandas_gbq.to_gbq(
        df, table_id, project_id, if_exists="append", credentials=credentials
    )
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
                job_detail_request(job_id, retry_count + 1)
            else:
                logging.error(
                    f"Failed to retrieve job_id: {job_id} after {retry_count} retries"
                )

        if response.status_code in [400, 404]:
            logging.warning(f"Job ID: {job_id} may be invalid or deleted.")
            return {"job_id": job_id, "description": ""}

    except Exception as e:
        logging.error(f"Error in job_detail_request: {e}")
        return {"job_id": job_id, "description": ""}

def enrich_jobs():
    # Load jobs without descriptions from BigQuery
    query = f"""
    SELECT job_id FROM `{project_id}.{table_id}`
    WHERE description IS NULL OR description = ""
    """
    jobs_df = pandas_gbq.read_gbq(query, project_id=project_id, credentials=credentials)
    job_ids = jobs_df["job_id"].tolist()

    batch_size = 50
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
