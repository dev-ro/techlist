import re
import requests
import ssl
import uuid
from bs4 import BeautifulSoup
import time
from fake_useragent import UserAgent
import logging
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

ssl._create_default_https_context = ssl._create_unverified_context
user_agent = UserAgent()

# Configure BigQuery credentials
credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")
project_id = "techlistme"
table_id = "raw_data.jobs"

def get_blacklist_companies(project_id="techlistme"):
    # SQL query to fetch company names from the blacklist table
    query = """
    SELECT company
    FROM `extracted_data.blacklist`
    """

    # Execute the query and load results into a DataFrame
    df = pandas_gbq.read_gbq(query, project_id=project_id, credentials=credentials)

    # Convert the 'company' column to a list
    blacklist_companies = df['company'].tolist()

    return blacklist_companies

blacklist = get_blacklist_companies()
blacklist = [company.lower() for company in blacklist]


def load_existing_job_ids():
    query = """
    WITH raw_jobs_count AS (
        SELECT COUNT(*) as count
        FROM `raw_data.jobs`
    ),
    all_jobs AS (
        SELECT job_id, 'raw' as source FROM `raw_data.jobs`
        UNION ALL
        SELECT job_id, 'extracted' as source FROM `extracted_data.jobs`
        UNION ALL
        SELECT job_id, 'bad' as source FROM `raw_data.bad_jobs`
    )
    SELECT DISTINCT job_id
    FROM all_jobs
    WHERE (SELECT count FROM raw_jobs_count) > 0
       OR source != 'raw'
    """
    df = pandas_gbq.read_gbq(query, project_id=project_id, credentials=credentials)
    logging.info(f"Loaded {len(df)} existing job IDs from BigQuery")
    return set(df["job_id"])

# Store existing job IDs in memory
existing_job_ids = load_existing_job_ids()


def job_exists(job_id):
    return job_id in existing_job_ids


def upload_to_bigquery(job_data):
    df = pd.DataFrame(job_data)
    pandas_gbq.to_gbq(
        df, table_id, project_id, if_exists="append", credentials=credentials
    )
    logging.info(f"Uploaded {len(job_data)} jobs to BigQuery")


def jobs_list_request(keyword, location, start=0):
    """Request to get job list from LinkedIn."""
    params = {
        "keywords": keyword,
        "location": location,
        "geoId": "",
        "trk": "public_jobs_jobs-search-bar_search-submit",
        "start": start,
    }
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    headers = {"User-Agent": user_agent.random}
    return requests.get(url=url, headers=headers, params=params)


def parse_job_list(keyword, location, page, task_id) -> list:
    job_data = []
    soup = BeautifulSoup(page, "html.parser")

    for container in soup.find_all(attrs={"class": "job-search-card"}):
        job_id = int(re.findall(r"\d+", container["data-entity-urn"])[0])
        title = container.find(attrs={"class": "base-search-card__title"}).text.strip()
        company = container.find(
            attrs={"class": "base-search-card__subtitle"}
        ).text.strip()

        # Skip jobs from companies on the blacklist
        if company.lower() in blacklist:
            logging.info(f"Skipping job from blacklisted company: {company}")
            continue

        # Check if job_id already exists in memory
        if not job_exists(job_id):
            job_data.append(
                {
                    "task_id": task_id,
                    "keyword": keyword,
                    "location": location,
                    "job_id": job_id,
                    "company": company,
                    "title": title,
                    "created_on": time.time(),
                }
            )

    return job_data


def process_jobs(keyword, location, task_id):
    start = 0
    all_job_data = []
    while True:
        prev = start

        response = jobs_list_request(keyword=keyword, location=location, start=start)

        if response.status_code != 200:
            if start >= 900:
                break

            logging.warning("Waiting ...")
            time.sleep(1.2)
            response = jobs_list_request(
                keyword=keyword, location=location, start=start
            )

        if response.status_code == 200:
            job_data = parse_job_list(
                keyword, location, page=str(response.content, "utf-8"), task_id=task_id
            )
            all_job_data.extend(job_data)

            start += 10

        logging.info(
            f"location: {location} status_code: {response.status_code} task_id: {task_id} page: {prev}-{start}"
        )

    # Upload accumulated job data to BigQuery after processing all pages for a keyword/location combination
    if all_job_data:
        upload_to_bigquery(all_job_data)
        logging.info(f"Uploaded batch of {len(all_job_data)} jobs to BigQuery")


if __name__ == "__main__":
    keywords = [
        "Data Scientist",
        "ML Engineer",
        "Data Analyst",
        "Data Engineer",
        "Business Analyst",
        "Software Engineer",
        "MLOps Engineer",
        "AI Engineer",
        "Decision Scientist",
    ]

    locations = [
        "Michigan",
        "Illinois",
        "California",
        "New York",
        "Washington",
        "Texas",
        "Florida",
        "Massachusetts",
        "Wisconsin",
        "Georgia",
        "Washington D.C.",
        "United States",
    ]

    for keyword in keywords:
        for location in locations:
            task_id = uuid.uuid4().hex
            logging.info(f"Collecting jobs - keyword: {keyword} - location: {location}")
            process_jobs(keyword, location, task_id)
