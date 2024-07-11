import os
import sys
from dotenv import load_dotenv
import json
import google.generativeai as genai
import time
import logging

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)


load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


def load_jobs(i):
    """Load jobs"""
    if os.path.exists(f"data/extracted/gemini/jobs_{i}.json"):
        with open(
            f"data/extracted/gemini/jobs_{i}.json", "r", encoding="utf-8"
        ) as file:
            logging.info(f"Loading from data/extracted/gemini/jobs_{i}.json")
            return clean(json.load(file))

    if os.path.exists(f"data/linkedin/jobs_{i}.json"):
        with open(f"data/linkedin/jobs_{i}.json", "r", encoding="utf-8") as file:
            logging.info(f"Loading from data/linkedin/jobs_{i}.json")
            return json.load(file)
    return []


def save_jobs(jobs, i):
    """Save jobs to linkedin_jobs.json with proper encoding."""
    with open(f"data/extracted/gemini/jobs_{i}.json", "w", encoding="utf-8") as file:
        json.dump(jobs, file, indent=4, ensure_ascii=False)


def extract_job_description(jobs, i):
    count_done = 0
    count_errors = 0

    for job in jobs:  # Adjust the range as needed
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
                                "experience" -> int (one number describing "years minimum exp". default 0), 
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
            save_jobs(jobs, i)
            count_done += 1

            elapsed = time.time() - start_time
            
            logging.info(f"job {job['job_id']} updated - elapsed: {elapsed:.2f} - progress: {count_done+count_errors}/{len(jobs)}")
            
        except Exception as e:
            count_errors += 1
            logging.error(f"job {job['job_id']} - total errors: {count_errors} - {e}")


def clean(jobs):
    # remove newlines, tabs, carriage returns
    for job in jobs:
        job["description"] = job["description"].replace("\n", " ").replace("\t", " ").replace("\r", " ")
        # logging.info(f"gemini job {job['job_id']} - cleaned")
    return jobs

if __name__ == "__main__":
    for i in range(49):
        jobs = load_jobs(i)
        # jobs = clean(jobs)
        # save_jobs(jobs, i)

        if not all(job.get("description") for job in jobs):
            logging.error(f"jobs_{i}.json has missing job descriptions. Exiting...")
            sys.exit("Restart scraper.py to scrape job descriptions first.")

        else:
            logging.info(f"jobs_{i}.json checked okay! Extracting job descriptions...")
            extract_job_description(jobs, i)
            logging.info(
                f"Updated job descriptions and saved to data/extracted/gemini/jobs_{i}.json"
            )
