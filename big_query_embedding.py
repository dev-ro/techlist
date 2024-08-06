from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("keys/gbq.json")
# Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project="techlistme")

import pandas as pd
from google.cloud import bigquery

# Sample 10,000 skills with embeddings from BigQuery
query = """
SELECT skill, embedding
FROM `techlistme.embeddings.hard_skills`
ORDER BY RAND()
LIMIT 10000
"""

print("Fetching sample from BigQuery...")
df = client.query(query).to_dataframe()

print("Sample fetched. Saving to CSV...")

# Save the sampled data to CSV
df.to_csv('data/hard_skills_embeddings_sample.csv', index=False)

print("Sample saved to 'hard_skills_embeddings_sample.csv'")
print(f"Number of rows in the sample: {len(df)}")
print(f"Number of unique skills in the sample: {df['skill'].nunique()}")