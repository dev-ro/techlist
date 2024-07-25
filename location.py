import requests
from bs4 import BeautifulSoup
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
from fake_useragent import UserAgent
import time

user_agent = UserAgent()

def job_detail_request(job_id, max_retries=3, base_delay=2):
    url = f"https://www.linkedin.com/jobs/view/{job_id}"
    for retry in range(max_retries):
        try:
            headers = {"User-Agent": user_agent.random}
            response = requests.get(url=url, headers=headers, timeout=5)
            print(f"Job ID: {job_id} Status Code: {response.status_code}")

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                location = soup.find(attrs={"class": "topcard__flavor--bullet"})
                location = location.text.strip() if location else "Location not found"
                return {"job_id": job_id, "location": location, "url": url}

            if response.status_code in [400, 404]:
                print(f"Job ID: {job_id} may be invalid or deleted.")
                return {"job_id": job_id, "location": "Job not found", "url": url}

            if response.status_code == 429:
                sleep_time = base_delay * (2 ** retry)
                print(f"Rate limited. Retrying after {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue

        except Exception as e:
            print(f"Error in job_detail_request for job_id {job_id}: {e}")

    print(f"Failed to retrieve job_id: {job_id} after {max_retries} retries")
    return {"job_id": job_id, "location": "Error occurred", "url": url}

def update_job_location(job_id, new_location, project_id="techlistme"):
    credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")

    client = bigquery.Client(project=project_id, credentials=credentials)
    table_id = f"{project_id}.extracted_data.jobs"

    query = f"""
    UPDATE `{table_id}`
    SET location = @new_location
    WHERE job_id = @job_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_location", "STRING", new_location),
            bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to complete

    print(f"Updated location for job_id {job_id} to '{new_location}'")

def get_current_job_info(job_id, project_id="techlistme"):
    query = f"""
    SELECT job_id, location
    FROM `{project_id}.extracted_data.jobs`
    WHERE job_id = @job_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        ]
    )

    df = pandas_gbq.read_gbq(query, project_id=project_id, configuration=job_config)
    if df.empty:
        return None
    return df.iloc[0]

def test_single_job_location():
    project_id = "techlistme"  # You can change this or make it a parameter
    job_id = input("Enter the job ID you want to test: ")

    # Get current job info from BigQuery
    current_job_info = get_current_job_info(job_id, project_id)
    if current_job_info is None:
        print(f"Job ID {job_id} not found in the database.")
        return

    print("\nCurrent job info in BigQuery:")
    print(f"Job ID: {current_job_info['job_id']}")
    print(f"Current Location: {current_job_info['location']}")

    # Scrape new location
    result = job_detail_request(job_id)
    print("\nScraping results:")
    print(f"Job ID: {result['job_id']}")
    print(f"Scraped Location: {result['location']}")
    print(f"URL: {result['url']}")

    # Update location if different
    if result['location'] != current_job_info['location'] and result['location'] != "Location not found":
        update = input("\nDo you want to update the location in BigQuery? (y/n): ")
        if update.lower() == 'y':
            update_job_location(job_id, result['location'], project_id)
            
            # Verify the update
            updated_job_info = get_current_job_info(job_id, project_id)
            print("\nUpdated job info in BigQuery:")
            print(f"Job ID: {updated_job_info['job_id']}")
            print(f"Updated Location: {updated_job_info['location']}")
    else:
        print("\nNo update needed. The scraped location is the same as in BigQuery or couldn't be found.")

if __name__ == "__main__":
    test_single_job_location()