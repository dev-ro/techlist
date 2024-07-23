import os
import sys
from dotenv import load_dotenv
import json
import google.generativeai as genai
import time
import logging
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import pandas_gbq

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Configure BigQuery credentials
credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")
project_id = "techlistme"
source_table_id = "raw_data.jobs"
destination_table_id = "extracted_data.jobs"

client = bigquery.Client(credentials=credentials, project=project_id)


def load_jobs(batch_size=10, offset=0):
    """Load jobs from BigQuery"""
    query = f"""
    SELECT job_id, description, task_id, keyword, location, company, title, created_on, url
    FROM `{project_id}.{source_table_id}`
    WHERE description IS NOT NULL AND description != ""
    ORDER BY created_on
    LIMIT {batch_size} OFFSET {offset}
    """
    return client.query(query).to_dataframe().to_dict(orient="records")


def save_jobs(df):
    """Save jobs to BigQuery"""
    pandas_gbq.to_gbq(
        df,
        destination_table_id,
        project_id,
        if_exists="append",
        credentials=credentials,
    )
    logging.info(f"Saved {len(jobs)} jobs to BigQuery")


def convert_column(column):
    if column.apply(type).eq(list).all():
        # Convert lists to comma-separated strings
        return column.apply(lambda x: ",".join(x) if isinstance(x, list) else x)
    elif column.apply(type).eq(dict).all():
        # Convert dicts to JSON strings
        return column.apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    return column


def convert_all_columns(data):
    for column in data.columns:
        data[column] = convert_column(data[column])
    return data


def extract_job_description(jobs):
    count_done = 0
    count_errors = 0
    updated_jobs = []

    for job in jobs:
        if "tech_stack" in job:
            count_done += 1
            continue

        try:
            start_time = time.time()

            model = genai.GenerativeModel(
                "models/gemini-1.5-flash-latest",
                generation_config={"response_mime_type": "application/json"},
                system_instruction="""
                                summarize/extract the data from this job description that returns a json with this exact schema: 
                                
                                "summary" -> string,
                                "industries" -> list<string> (how? infer the related industries),
                                "soft_skills" -> list<string> (how? briefly list each skill), 
                                "hard_skills" -> list<string> (how? briefly list each skill), 
                                "tech_stack" -> list<string>, 
                                "programming_languages" -> list<string>, 
                                "education": ("min_degree" -> string, "fields" -> list<string>), 
                                "salary": ("max" -> int, "min" -> int) (how? look for $ pay or compensation. default 0 if not mentioned), 
                                "benefits" -> list<string> (how? briefly list each benefit).
                                
                                tech_stack is the most important field, so look carefully for any tech stack related information

                                the following is an example of bad output:
                                "hard_skills": [
                                    "Programming languages: Python, R, C, Java",
                                    "Big data platforms: Map/Reduce, YARN, HDFS",
                                    "Query building: PL/SQL, HIVE, Impala, SparkSQL",
                                    "Parallel compute frameworks",
                                    "Visualization tools: QlikView, Tableau, Web FOCUS",
                                    "Statistical modeling: linear and non-linear regression, neural networks, logistic regression, decision trees, gradient boosting machines, support vector machines, random forests",
                                    "Statistical testing techniques: odds-ratios, t-tests, chi-squared, ANOVA",
                                    "Code version control systems: git/GitHub",
                                    "Microsoft Access, Word, PowerPoint, and Excel"
                                ],

                                the following is an example of good output:
                                "hard_skills": [
                                    "Python", "R", "C", "Java", "Map/Reduce", "YARN", "HDFS",
                                    "PL/SQL", "HIVE", "Impala", "SparkSQL", "QlikView", "Tableau", "Web FOCUS",
                                    "Regression", "Neural Networks", "Logistic Regression", "Decision Trees", 
                                    "Gradient Boosting Machines", "Support Vector Machines", "Random Forests",
                                    "Odds-Ratios", "T-Tests", "Chi-Squared", "ANOVA", "Git",
                                    "Access", "Word", "PowerPoint", "Excel"
                                ],           
                                """,
            )
            response = model.generate_content(job["description"])
            new_fields = json.loads(response.text)
            job.update(new_fields)
            job["created_on"] = time.time()
            updated_jobs.append(job)
            count_done += 1

            elapsed = time.time() - start_time
            logging.info(
                f"job {job['job_id']} updated - elapsed: {elapsed:.2f} - progress: {count_done+count_errors}/{len(jobs)}"
            )

        except Exception as e:
            count_errors += 1
            logging.error(f"job {job['job_id']} - total errors: {count_errors} - {e}")

    updated_jobs = convert_all_columns(pd.DataFrame(updated_jobs))
    save_jobs(updated_jobs)


def clean(jobs):
    # Remove newlines, tabs, carriage returns
    for job in jobs:
        job["description"] = (
            job["description"].replace("\n", " ").replace("\t", " ").replace("\r", " ")
        )
    return jobs


if __name__ == "__main__":
    batch_size = 100
    offset = 0

    while True:
        jobs = load_jobs(batch_size=batch_size, offset=offset)
        if not jobs:
            break
        jobs = clean(jobs)

        if not all(job.get("description") for job in jobs):
            logging.error("Some jobs have missing descriptions. Exiting...")
            sys.exit("Restart the pipeline to scrape job descriptions first.")

        else:
            logging.info(f"Processing batch starting at offset {offset}...")
            extract_job_description(jobs)
            logging.info(
                f"Processed batch starting at offset {offset} and saved to BigQuery"
            )

        offset += batch_size

    logging.info("All jobs processed and saved to BigQuery")
