# Techlist.me

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

## Note on Data and Credentials

The application is designed to work with BigQuery and not local data files. Access to the full functionality requires appropriate GCP credentials and BigQuery setup, which are not provided in this repository for security reasons.

## Data Refresh

Job posting data is automatically updated on a weekly basis using a Kubernetes-based web scraping system.
