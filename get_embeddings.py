import numpy as np
import pandas as pd
from tqdm import tqdm
from openai import OpenAI
import ast
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def embed_texts(texts):
    if not texts:
        return []
    try:
        response = client.embeddings.create(model="text-embedding-3-small", input=texts)
        return [item.embedding for item in response.data]
    except Exception as e:
        print(f"Error in embed_texts: {e}")
        return []


def preprocess_and_embed_individual_skills(skills_string):
    if not skills_string or pd.isna(skills_string):
        return []

    try:
        skills_list = ast.literal_eval(skills_string)
    except:
        skills_list = [s.strip() for s in skills_string.split(",") if s.strip()]

    if not skills_list:
        return []

    embeddings = embed_texts(skills_list)
    return list(zip(skills_list, embeddings))


def generate_and_save_embeddings(df, column_name, file_name):
    all_skills_embeddings = []
    skills_strings = df[column_name].tolist()
    for idx, skills_string in tqdm(
        enumerate(skills_strings), total=len(skills_strings)
    ):
        try:
            skill_embeddings = preprocess_and_embed_individual_skills(skills_string)
            all_skills_embeddings.extend(skill_embeddings)
        except Exception as e:
            print(f"Error processing index {idx}: {e}")

    # Create DataFrame with individual skills and their embeddings
    skills_df = pd.DataFrame(all_skills_embeddings, columns=["skill", "embedding"])
    skills_df["embedding"] = skills_df["embedding"].apply(
        lambda x: ",".join(map(str, x))
    )
    skills_df.to_csv(file_name, index=False)
    return skills_df


# Usage example
def main():
    df = pd.read_csv("data/full_data.csv")
    column_name = "hard_skills"
    output_file = f"data/{column_name}_embeddings.csv"

    skills_df = generate_and_save_embeddings(df, column_name, output_file)
    print(f"Individual skill embeddings saved to {output_file}")


if __name__ == "__main__":
    main()
