import re
import requests
import ssl
import uuid
from bs4 import BeautifulSoup
import time
from fake_useragent import UserAgent
import logging
import json
import os
import pandas as pd

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

ssl._create_default_https_context = ssl._create_unverified_context
user_agent = UserAgent()

all_jobs = []
blacklist = ["SynergisticIT", "Intellectt Inc"]


def load_jobs_from_pkl():
    global all_jobs
    if os.path.exists("data/linkedin/jobs.pkl"):
        all_jobs_df = pd.read_pickle("data/linkedin/jobs.pkl")
        all_jobs = all_jobs_df.to_dict("records")


def save_jobs_to_pkl():
    global all_jobs
    all_jobs_df = pd.DataFrame(all_jobs)
    all_jobs_df.to_pickle("data/linkedin/jobs.pkl")


def save_jobs_to_json():
    global all_jobs
    chunk_size = 500
    for i in range(0, len(all_jobs), chunk_size):
        chunk = all_jobs[i : i + chunk_size]
        with open(f"data/linkedin/jobs_{i//chunk_size}.json", "w", encoding="utf-8") as file:
            json.dump(chunk, file, indent=4, ensure_ascii=False)


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


def job_detail_request(job_id, retry_count=0) -> None:
    """Request to get job details from LinkedIn."""
    global all_jobs
    url = f"https://www.linkedin.com/jobs/view/{job_id}"
    try:
        start_time = time.time()
        headers = {"User-Agent": user_agent.random}
        response = requests.get(url=url, headers=headers, timeout=5)
        elapsed = time.time() - start_time
        logging.info(
            f"job_id: {job_id} status_code: {response.status_code} elapsed: {elapsed:.2f}"
        )

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            company = soup.find(attrs={"class": "topcard__flavor--black-link"})
            if not company:
                company = soup.find(attrs={"class": "topcard__flavor"})
            company = company.text.strip()
            location = soup.find(
                attrs={"class": "topcard__flavor--bullet"}
            ).text.strip()
            description = soup.find(attrs={"class": "show-more-less-html__markup"})
            description = description.getText(separator="\n", strip=True)

            # Delete jobs from companies on the blacklist
            if company in blacklist:
                logging.info(
                    f"Skipping job_id: {job_id} from blacklist company: {company}"
                )
                all_jobs = [job for job in all_jobs if job["job_id"] != job_id]

                # Save jobs incrementally
                save_jobs_to_pkl()
                save_jobs_to_json()
                return

            
            if any(job.get('description') == description for job in all_jobs):
                logging.info(f"Deleting duplicate by description - job_id: {job_id} company: {company}")
                all_jobs = [job for job in all_jobs if job["job_id"] != job_id]
                
                # Save jobs incrementally
                save_jobs_to_pkl()
                save_jobs_to_json()
                return

            for job in all_jobs:
                if job["job_id"] == job_id:
                    job["company"] = company
                    job["location"] = location
                    job["description"] = description
                    job["url"] = url
                    job["created_on"] = time.time()

            # Save jobs incrementally
            save_jobs_to_pkl()
            save_jobs_to_json()

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
            logging.warning(f"Deleting job_id: {job_id}")
            all_jobs = [job for job in all_jobs if job["job_id"] != job_id]

            # Save jobs incrementally
            save_jobs_to_pkl()
            save_jobs_to_json()
    except Exception as e:
        logging.error(f"Error in job_detail_request: {e}")


def parse_job_list(keyword, location, page, task_id) -> None:
    global all_jobs
    soup = BeautifulSoup(page, "html.parser")

    for container in soup.find_all(attrs={"class": "job-search-card"}):
        job_id = int(re.findall(r"\d+", container["data-entity-urn"])[0])
        if any(job["job_id"] == job_id for job in all_jobs):
            continue
        title = container.find(attrs={"class": "base-search-card__title"}).text.strip()
        company = container.find(
            attrs={"class": "base-search-card__subtitle"}
        ).text.strip()

        # Skip jobs from companies on the blacklist
        if company in blacklist:
            continue

        all_jobs.append(
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

        logging.info(f"job_id: {job_id} title: {title}")


def process_jobs(keyword, location, task_id):
    start = 0
    while True:
        prev = start

        response = jobs_list_request(keyword=keyword, location=location, start=start)

        if response.status_code != 200:
            if start >= 990:  # Changed limit to 990
                break

            logging.warning("Waiting ...")
            time.sleep(1.2)
            response = jobs_list_request(
                keyword=keyword, location=location, start=start
            )

        if response.status_code == 200:
            parse_job_list(
                keyword, location, page=str(response.content, "utf-8"), task_id=task_id
            )

            start += 10  # Increment by 10

        logging.info(
            f"location: {location} status_code: {response.status_code} task_id: {task_id} page: {prev}-{start}"
        )

        # Save jobs incrementally
        save_jobs_to_pkl()
        save_jobs_to_json()


if __name__ == "__main__":
    load_jobs_from_pkl()  # Load existing jobs from PKL before starting

    keywords = [
        # "Data Scientist",
        # "ML Engineer",
        # "Data Analyst",
        # "Data Engineer",
        # "Business Analyst",
        # "Software Engineer",
        # "MLOps Engineer",
        # "AI Engineer",
        # "Decision Scientist",
    ]

    locations = [
        # "Michigan",
        # "Illinois",
        # "California",
        # "New York",
        # "Washington",
        # "Texas",
        # "Florida",
        # "Massachusetts",
        # "Wisconsin",
        # "Georgia",
        # "Washington D.C.",
        # "United States",
    ]

    # for keyword in keywords:
    #     for location in locations:
    #         task_id = uuid.uuid4().hex
    #         logging.info(f"total jobs: {len(all_jobs)} - keyword: {keyword} - location: {location}")

    #         process_jobs(keyword, location, task_id)

    # for job in all_jobs:
    #     # print(job)
    #     if 'description' not in job:
    #         job_detail_request(job["job_id"])

    df = pd.DataFrame(all_jobs)
    # convert NaN to "" for description
    df["description"] = df["description"].fillna("")
    all_jobs = df.to_dict("records")

    for job in all_jobs:
        if job["description"] == "":
            job_detail_request(job["job_id"])
            count_jobs = len(all_jobs)
            count_jobs_with_description = len([job for job in all_jobs if job["description"] != ""])
            if count_jobs_with_description % 10 == 0:
                logging.info(f"{count_jobs_with_description}/{count_jobs} jobs have descriptions.")

    for job in all_jobs:
        # remove newlines from description
        job["description"] = job["description"].replace("\n", " ")
                    
    # Final save of all jobs to ensure the latest data is saved
    save_jobs_to_pkl()
    save_jobs_to_json()   

    logging.info(f"Final scraped job data has been saved. Total jobs: {len(all_jobs)}")
