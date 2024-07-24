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

blacklist = [
    "synergisticit",
    "intellectt inc",
    "clearancejobs",
    "steneral consulting",
    "syntricate technologies",
    "1872 consulting",
    "acs consultancy services, inc",
    "mygwork - lgbtq+ business community",
    "zortech solutions",
    "energy jobline",
    "kyyba inc",
    "motion recruitment",
    "jobs via efinancialcareers",
    "robert half",
    "accroid inc",
    "stellent it",
    "software technology inc.",
    "donato technologies, inc.",
    "tekintegral",
    "extend information systems inc.",
    "keylent inc",
    "kforce inc",
    "ampcus inc",
    "get it recruit - information technology",
    "cybercoders",
    "diverse lynx",
    "remoteworker us",
    "harnham",
    "augment jobs",
    "tata consulting company",
    "tata consultancy services",
    "clickjobs.io",
    "jobot",
    "TekWissen ®",
    "dice",
    "techtammina llc",
    "cynet systems",
    "iconma",
    "spectraforce",
    "agile tech labs",
    "genesis10",
    "insight global",
    "ceres group",
    "smartiplace",
    "jobs malaysia - two95 hr hub",
    "stellar professionals",
    "lancesoft, inc.",
    "divihn integration inc",
    "wise skulls",
    "cybertec, inc",
    "lorven technologies inc.",
    "georgia it, inc.",
    "avid technology professionals",
    "hcl global systems inc",
    "excel hire staffing,llc",
    "capgemini",
    "randstad usa",
    "v-soft consulting group, inc.",
    "mission technologies, a division of hii",
    "prohires",
    "roberts recruiting, llc",
    "caci international inc",
    "mantech",
    "belay technologies",
    "mindlance",
    "psrtek",
    "info way solutions",
    "the judge group",
    "ziprecruiter",
    "hexaquest global",
    "captivation",
    "conch technologies, inc",
    "open systems technologies",
    "acceler8 talent",
    "alldus",
    "clifyx",
    "marathon ts",
    "aptask",
    "v2soft",
    "hatchpros",
    "aditi consulting",
    "ltimindtree",
    "software people inc.",
    "lasalle network",
    "compunnel inc.",
    "guidehouse",
    "intersources inc",
    "ev.careers",
    "resource informatics group, inc",
    "htc global services",
    "pyramid consulting, inc",
    "artech l.l.c.",
    "axelon services corporation",
    "pi square technologies",
    "enexus global inc.",
    "algo capital group",
    "anveta, inc",
    "akraya, inc.",
    "softworld, a kelly company",
    "ascendion",
    "akkodis",
    "fasttek global",
    "system soft technologies",
    "sky consulting inc.",
    "intelliswift software",
    "qinetiq us (formerly avantus federal)",
    "lhh",
    "chelsoft solutions co.",
    "serigor inc",
    "us tech solutions",
    "inspyr solutions",
    "amtex systems inc.",
    "shiftcode analytics, inc.",
    "etek it services, inc.",
    "integrated resources, inc ( iri )",
    "eteam",
    "applab systems, inc",
    "selby jennings",
    "lmi",
    "cps, inc.",
    "mindpal",
    "usajobs",
    "caterpillar inc.",
    "systems technology group, inc. (stg)",
    "team remotely inc",
    "megan soft inc",
    "inficare staffing",
    "dcs corp",
    "hexaware technologies",
    "stanley reid",
    "epitec",
    "trispoke managed services pvt. ltd.",
    "actalent",
    "jesica.ai",
    "stealth startup",
    "paradyme, inc.",
    "qinetiq us",
]


# Load existing job IDs from BigQuery once
def load_existing_job_ids():
    query = f"""
    SELECT job_id FROM `{project_id}.{table_id}`
    """
    df = pandas_gbq.read_gbq(query, project_id=project_id, credentials=credentials)
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
