# [Techlist.me](https://techlist.me)

## Overview

Techlist.me is a data-driven web application designed to analyze the technology landscape for job seekers in the data science field. By leveraging web scraping, natural language processing, and data analytics techniques, Techlist.me provides up-to-date insights into the most in-demand skills, technologies, and industries in the data science job market.

## Features

- **Interactive Dashboard**: Visualizes key job market trends including:
  - Top tech stacks
  - Most in-demand hard skills
  - Frequently mentioned soft skills
  - Companies with the most job postings
  - Leading industries for data science roles
  - Common benefits offered

- **Automated Data Collection**: Utilizes a Kubernetes cluster to perform weekly web scraping of LinkedIn job postings, ensuring the data remains current.

- **Advanced Natural Language Processing**: Employs Google Gemini Flash for efficient and accurate keyword extraction from job postings.

- **Cloud-Based Infrastructure**: Hosted on Google Cloud Platform, utilizing App Engine for hosting and BigQuery for data storage.

## Technology Stack

- **Application Framework**: Streamlit
- **Data Processing**: Python (pandas, pandas_gbq)
- **Visualization**: Altair
- **Database**: Google BigQuery
- **Cloud Platform**: Google Cloud Platform (App Engine, Kubernetes Engine, BigQuery)
- **NLP Model**: Google Gemini Flash

## Usage

To use Techlist.me, simply visit [techlist.me](https://techlist.me) in your web browser. The interactive dashboard allows you to:

- Filter data by job type and company
- Visualize top keywords for each category (tech stack, hard skills, soft skills, company, industries, benefits)
- Explore trends and insights in the data science job market

## Setup and Installation

For those interested in running the application locally or deploying it, here's a high-level overview of the setup process:

1. Clone the repository
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Enable GCP APIs for BigQuery
4. Set up GCP credentials for BigQuery and save to `keys/gbq.json`
5. Install Google Cloud CLI

### Running Locally

To run the app locally:

1. Navigate to the app folder
2. Run the command:

   ```bash
   streamlit run app.py
   ```

### Deploying to Google App Engine

To deploy the app to Google App Engine:

1. Navigate to the app folder
2. Run the command:

   ```bash
   gcloud app deploy
   ```

This will dockerize the application and deploy it to Google App Engine.

Optional: To connect your own domain to the App Engine hosted domain, follow Google Cloud's documentation on custom domain setup.

### Data Pipeline

The data extraction pipeline is deployed on Google Kubernetes Engine and executes in the following order once a week:

1. collect_job_listings - Here I collect the job_ids from a LinkedIn search based on Keyword and Location.
2. clean_duplicate_ids - Searches often have job_ids you already found; this step is for efficiency
3. enrich_job_listings - This step is going to each job url created from the job_ids it found, and saving its description to the database.
4. clean_duplicate_descriptions - Some jobs, although having unique job_ids will have identical descriptions, so remove those.
5. extract_gemini - This step is extracting the keywords from the job descriptions with Google Gemini API and putting it into the extracted_data.jobs table, used for the website.
6. clean_duplicate_descrptions - This container also has a method for cleaning up the raw_data.jobs table if the job keywords have been extracted, so it runs again as a final step.

## Note on Data and Credentials

The application is designed to work with BigQuery and not local data files. Access to the full functionality requires appropriate GCP credentials and BigQuery setup, which are not provided in this repository for security reasons. A 100-row sample of the dataset is provided at data/sample.csv

## Data Refresh

Job posting data is automatically updated on a weekly basis using a Kubernetes-based web scraping system.
