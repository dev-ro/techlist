import pandas as pd
import numpy as np
import openai
import json
import os
from tqdm import tqdm
from sklearn.cluster import KMeans
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from collections import Counter
from dotenv import load_dotenv
from openai import OpenAI
import tiktoken

load_dotenv()

# Directory containing the JSON files
directory = 'data/extracted/gemini'

# List to hold all job entries
all_jobs = []

# Loop through the files and read each JSON file
for filename in os.listdir(directory):
    if filename.endswith('.json'):
        with open(os.path.join(directory, filename), 'r', encoding='utf-8') as file:
            jobs = json.load(file)
            all_jobs.extend(jobs)

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(all_jobs)
jobs_df = df.copy()

# Initialize the OpenAI client with the API key directly
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Initialize the tokenizer for the specified model
encoding = tiktoken.encoding_for_model("text-embedding-3-small")

# Function to ensure the input does not exceed the max token limit
def truncate_texts(texts, max_tokens=8191):
    tokens = []
    for text in texts:
        if not isinstance(text, str):
            continue  # Skip non-string inputs
        tokens += encoding.encode(text) + [encoding.encode(" ")[0]]  # Add a space token between texts
        if len(tokens) > max_tokens:
            tokens = tokens[:max_tokens]
            break
    return encoding.decode(tokens)

# Function to generate embeddings for a list of texts using OpenAI API (batch processing)
def embed_texts(texts):
    truncated_text = truncate_texts(texts)
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=[truncated_text]
    )
    return [item.embedding for item in response.data]

# Function to preprocess and embed each field
def preprocess_and_embed(skills_list):
    if len(skills_list) == 0:
        return np.zeros((1536,))  # Adjust the dimension according to the model's output
    embeddings = embed_texts(skills_list)
    return np.mean(embeddings, axis=0)  # Average the embeddings for the list

# Generate and save embeddings to CSV
def generate_and_save_embeddings(df, column_name, file_name):
    embeddings = []
    skills_lists = df[column_name].tolist()
    for idx, skills_list in tqdm(enumerate(skills_lists), total=len(skills_lists)):
        try:
            if isinstance(skills_list, list):
                embedding = preprocess_and_embed(skills_list)
            else:
                embedding = embed_texts([skills_list])[0]
        except Exception as e:
            print(f"Error processing index {idx}: {e}")
            embedding = np.zeros((1536,))  # Use a zero vector in case of error
        embeddings.append(embedding)
    embeddings_df = pd.DataFrame(embeddings)
    embeddings_df.to_csv(file_name, index=False)
    return embeddings_df

# Assuming jobs_df is already defined and populated with job data
# hard_skills_embeddings_df = generate_and_save_embeddings(jobs_df, 'hard_skills', 'data/embeddings/hard_skills_embeddings.csv')
tech_stack_embeddings_df = generate_and_save_embeddings(jobs_df, 'tech_stack', 'data/embeddings/tech_stack_embeddings.csv')
soft_skills_embeddings_df = generate_and_save_embeddings(jobs_df, 'soft_skills', 'data/embeddings/soft_skills_embeddings.csv')
industries_embeddings_df = generate_and_save_embeddings(jobs_df, 'industries', 'data/embeddings/industries_embeddings.csv')
