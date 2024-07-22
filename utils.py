import pandas as pd
import json
import streamlit as st
import os


# Directory containing the JSON files
directory = "data/extracted/gemini"


def load_json_data():
    # List to hold all job entries
    all_jobs = []
    # Loop through the files and read each JSON file
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            with open(os.path.join(directory, filename), "r", encoding="utf-8") as file:
                jobs = json.load(file)
                all_jobs.extend(jobs)

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(all_jobs)
    return df


data = load_json_data()
data = data.dropna(subset=["summary"])

# Display the DataFrame
# st.write(data)

import pandas as pd
import json


def convert_column(column):
    if column.apply(type).eq(list).all():
        # Convert lists to comma-separated strings
        return column.apply(lambda x: ",".join(x) if isinstance(x, list) else x)
    elif column.apply(type).eq(dict).all():
        # Convert dicts to JSON strings
        return column.apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    return column


for column in data.columns:
    data[column] = convert_column(data[column])

# drop column "experience"
data = data.drop(columns=["experience"])

from google.oauth2 import service_account
import pandas_gbq

credentials = service_account.Credentials.from_service_account_file(
    "keys/gbq.json",
)

# TODO: Set project_id to your Google Cloud Platform project ID.
project_id = "techlistme"
# TODO: Set table_id to the full destination table ID (including the
#       dataset ID).
table_id = "extracted_data.jobs"

pandas_gbq.to_gbq(
    data, table_id, project_id, if_exists="append", credentials=credentials
)
