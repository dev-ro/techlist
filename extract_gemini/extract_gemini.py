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
bad_jobs_table_id = "raw_data.bad_jobs"

client = bigquery.Client(credentials=credentials, project=project_id)


def load_remaining_jobs(batch_size=10):
    """Load remaining jobs from BigQuery"""
    query = f"""
    WITH remaining_jobs AS (
        SELECT r.job_id, r.description, r.task_id, r.keyword, r.location, r.company, r.title, r.created_on, r.url
        FROM `{project_id}.{source_table_id}` r
        LEFT JOIN `{project_id}.{destination_table_id}` e ON r.job_id = e.job_id
        LEFT JOIN `{project_id}.{bad_jobs_table_id}` b ON r.job_id = b.job_id
        WHERE e.job_id IS NULL AND b.job_id IS NULL
        AND r.description IS NOT NULL AND r.description != ""
    )
    SELECT *
    FROM remaining_jobs
    ORDER BY created_on
    LIMIT {batch_size}
    """
    df = client.query(query).to_dataframe()
    df['job_id'] = df['job_id'].astype(int)  # Ensure job_id is treated as an integer
    return df.to_dict(orient="records")


def save_jobs(df, table_id):
    """Save jobs to BigQuery"""
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id,
        if_exists="append",
        credentials=credentials,
    )
    logging.info(f"Saved {len(df)} jobs to {table_id}")


def delete_jobs_from_raw(job_ids):
    """Delete jobs from raw_data.jobs"""
    job_ids_str = ', '.join(str(id) for id in job_ids)  # Convert to string for SQL
    delete_query = f"""
    DELETE FROM `{project_id}.{source_table_id}`
    WHERE job_id IN ({job_ids_str})
    """
    client.query(delete_query).result()
    logging.info(f"Deleted {len(job_ids)} jobs from {source_table_id}")


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
    bad_jobs = []

    for job in jobs:
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
            job["error"] = str(e)
            bad_jobs.append(job)

    if updated_jobs:
        updated_jobs_df = convert_all_columns(pd.DataFrame(updated_jobs))
        save_jobs(updated_jobs_df, destination_table_id)

    if bad_jobs:
        bad_jobs_df = pd.DataFrame(bad_jobs)
        save_jobs(bad_jobs_df, bad_jobs_table_id)
        delete_jobs_from_raw([job["job_id"] for job in bad_jobs])


def clean(jobs):
    # Remove newlines, tabs, carriage returns
    for job in jobs:
        job["description"] = (
            job["description"].replace("\n", " ").replace("\t", " ").replace("\r", " ")
        )
    return jobs


if __name__ == "__main__":
    batch_size = 100

    while True:
        jobs = load_remaining_jobs(batch_size=batch_size)
        if not jobs:
            break
        jobs = clean(jobs)

        if not all(job.get("description") for job in jobs):
            logging.error("Some jobs have missing descriptions. Exiting...")
            sys.exit("Restart the pipeline to scrape job descriptions first.")

        else:
            logging.info(f"Processing batch of {len(jobs)} remaining jobs...")
            extract_job_description(jobs)
            logging.info(f"Processed batch of {len(jobs)} jobs")

    logging.info("All remaining jobs processed")
