from groq import Groq
import os
from dotenv import load_dotenv
import json

load_dotenv()

# Initialize the Groq client
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def load_jobs():
    """Load jobs from linkedin_jobs.json."""
    if os.path.exists("linkedin_jobs.json"):
        with open("linkedin_jobs.json", "r") as file:
            return json.load(file)
    return []

def save_jobs(jobs):
    """Save jobs to linkedin_jobs.json."""
    with open("linkedin_jobs.json", "w") as file:
        json.dump(jobs, file, indent=4)

def extract_job_description(jobs):
    """Extract job description from job list and update jobs with new fields."""
    for job in jobs[:5]:  # Adjust the range as needed
        if job.get("company") == "SynergisticIT":
            continue
        try:
            completion = client.chat.completions.create(
                model="llama3-8b-8192",
                messages=[
                    {
                        "role": "system",
                        "content": """
                        
                        summarize/extract the data from this job description that returns a json with this exact schema: 
                        
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
                            "PL", "SQL", "HIVE", "Impala", "SparkSQL", "QlikView", "Tableau", "Web FOCUS",
                            "regression", "neural networks", "logistic regression", "decision trees", 
                            "gradient boosting machines", "support vector machines", "random forests",
                            "odds-ratios", "t-tests", "chi-squared", "ANOVA", "git",
                            "Access", "Word", "PowerPoint", "Excel"
                        ],
                       
                        """
                    },
                    {
                        "role": "user",
                        "content": f""" 
                        
                        "description": {job['description']}

                        """
                    },
                ],
                temperature=0.44,
                max_tokens=8192,
                top_p=0.77,
                stream=False,
                response_format={"type": "json_object"},
                stop=None,
            )

            # Parse the response and update the job entry
            new_fields = json.loads(completion.choices[0].message.content)
            job.update(new_fields)

        except Exception as e:
            print(f"Error extracting job description for job ID {job['job_id']}: {e}")

if __name__ == "__main__":
    jobs = load_jobs()
    extract_job_description(jobs)
    save_jobs(jobs)
    print("Updated job descriptions and saved to linkedin_jobs.json")
