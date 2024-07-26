import pandas as pd
import json
import streamlit as st
from collections import Counter
import altair as alt
import numpy as np

from google.oauth2 import service_account
import pandas_gbq


# Set page configuration
st.set_page_config(
    page_title="techlist.me",  # Title of the web page
    page_icon="🖥️",  # Favicon (optional), can be an emoji or a path to an image file
    layout="centered",  # Use "wide" or "centered" layout
    menu_items={
        "About": """
        ## Techlist.me
        This application provides insights into the technology stack and skill requirements for data-related job postings.
        Created by Kyle as part of the Master of Applied Data Science program at the University of Michigan School of Information.
        """
    },
)


def load_markdown(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


with st.expander("Introduction"):
    intro = load_markdown("data/intro.md")
    intro

with st.container():

    @st.cache_data(ttl=3600)  # Cache data for 1 hour
    def load_data():
        sql = """
        SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', 
            TIMESTAMP_MILLIS(CAST(created_on * 1000 AS INT64)), 
            "America/New_York") AS time_extracted,
        keyword, company, title, summary, url, 
        hard_skills, tech_stack, soft_skills, 
        industries, benefits, salary
        FROM `extracted_data.jobs` 
        ORDER BY created_on
        """

        credentials = service_account.Credentials.from_service_account_file(
            "keys/gbq.json",
        )

        project_id = "techlistme"

        # Load data from BigQuery
        data = pandas_gbq.read_gbq(
            sql,
            project_id=project_id,
            credentials=credentials,
            use_bqstorage_api=True,
        )
        return data

    data = load_data()
    data = data.dropna(subset=["summary"])

    def convert_strings_to_lists(df, column_name):
        df[column_name] = df[column_name].apply(
            lambda x: list(set(x.split(","))) if pd.notna(x) else []
        )
        return df

    columns_with_lists = [
        "tech_stack",
        "soft_skills",
        "hard_skills",
        "industries",
        "benefits",
    ]
    # Convert column
    for column in columns_with_lists:
        data = convert_strings_to_lists(data, column)

    # Function to convert string to dictionary
    def convert_salary_string_to_dict(salary_str):
        try:
            return json.loads(salary_str)
        except json.JSONDecodeError:
            return {}

    # Apply conversion to the salary column
    data["salary"] = data["salary"].apply(convert_salary_string_to_dict)

    # Function to replace words in lists
    def replace_words_in_list(data, column_name, replacements):
        # lowercase all replacements
        replacements = {
            key.lower(): value.lower() for key, value in replacements.items()
        }

        for i, items in data[column_name].items():

            # lowercase all items
            if isinstance(items, list):
                items = [item.lower() for item in items if isinstance(item, str)]

            if isinstance(items, list):
                data.at[i, column_name] = [
                    replacements.get(item, item) for item in items
                ]
            elif isinstance(items, str):
                data.at[i, column_name] = replacements.get(items, items)

    # Dictionary for replacements
    replacements = {
        "PowerBI": "Power BI",
        "401k": "401(k)",
        "401(k) plan": "401(k)",
        "Vision Insurance": "Vision",
        "Dental Insurance": "Dental",
        "Medical Insurance": "Medical",
        "Microsoft Excel": "Excel",
        "Microsoft Office Suite": "Microsoft Office",
        "Microsoft Word": "Word",
        "Tuition Assistance": "Tuition Reimbursement",
        "PTO": "Paid Time Off",
        "Competitive Salary": "Competitive Compensation",
        "Paid Parental Leave": "Parental Leave",
        "Attention to Detail": "Detail Oriented",
        "Detail-oriented": "Detail Oriented",
        "problem-solving": "Problem Solving",
        "analytical thinking": "Analytical",
        "analytical skills": "Analytical",
        "Presentation": "Presentation Skills",
        "apache spark": "Spark",
        "apache kafka": "Kafka",
        "apache hadoop": "Hadoop",
        "apache airflow": "Airflow",
        "azure databricks": "Databricks",
        "google cloud platform": "GCP",
        "amazon web services": "AWS",
        "aws services": "AWS",
        "interpersonal skills": "interpersonal",
    }

    # Replace words in the specified columns
    columns_to_replace = [
        "hard_skills",
        "tech_stack",
        "soft_skills",
        "industries",
        "company",
        "benefits",
    ]
    for column in columns_to_replace:
        if column in data.columns:
            replace_words_in_list(data, column, replacements)

    def get_blacklist_companies(project_id="techlistme"):
        # SQL query to fetch company names from the blacklist table
        query = """
        SELECT company
        FROM `extracted_data.blacklist`
        """

        # Execute the query and load results into a DataFrame
        df = pandas_gbq.read_gbq(query, project_id=project_id)

        # Convert the 'company' column to a list
        blacklist_companies = df["company"].tolist()

        return blacklist_companies

    # Blacklist of companies
    blacklist_companies = get_blacklist_companies()

    # Convert company names to lowercase for case-insensitive comparison
    data["company"] = data["company"].str.lower()

    blacklist_companies = [company.lower() for company in blacklist_companies]

    # Calculate the number of excluded jobs before filtering
    excluded_jobs_count = data[data["company"].isin(blacklist_companies)].shape[0]

    # Filter out blacklisted companies
    data = data[~data["company"].isin(blacklist_companies)]

    # Streamlit app layout
    st.title("Dashboard")

    # Get the unique keywords for the drop-down menu and add "All"
    unique_keywords = ["All Data Related Jobs"] + data["keyword"].unique().tolist()

    # Drop-down menu for selecting a keyword
    keyword = st.selectbox("Select a job title keyword", unique_keywords)

    # Filter data based on the selected keyword
    if "All" in keyword:
        filtered_data = data
    else:
        filtered_data = data[data["keyword"] == keyword]

    # Function to count frequency of words in lists, case-insensitively
    def count_frequency(column_data):
        all_items = []
        for item in column_data:
            if isinstance(item, list):
                # Filter out empty lists and extend with lowercased items
                if item:
                    all_items.extend([i.lower() for i in item if i])
            elif isinstance(item, str):
                if item:
                    all_items.append(item.lower())
        return Counter(all_items)

    # Function to calculate median min and max salary
    def calculate_mean_salary(data):
        min_salaries = [
            entry["min"]
            for entry in data
            if isinstance(entry, dict)
            and "min" in entry
            and entry["min"] is not None
            and entry["min"] >= 20000
        ]
        max_salaries = [
            entry["max"]
            for entry in data
            if isinstance(entry, dict)
            and "max" in entry
            and entry["max"] is not None
            and entry["max"] >= 20000
        ]

        min_salary = np.mean(min_salaries) if min_salaries else 0
        max_salary = np.mean(max_salaries) if max_salaries else 0

        return min_salary, max_salary

    # Extract salary column from the filtered DataFrame
    salary_data = filtered_data["salary"].tolist()

    # Calculate median min and max salary
    min_salary, max_salary = calculate_mean_salary(salary_data)

    # Display the results in Streamlit
    st.write(f"Average Minimum Salary for {keyword}: ${min_salary:,.2f}")
    st.write(f"Average Maximum Salary for {keyword}: ${max_salary:,.2f}")

    st.write(f"Count of Job Postings for {keyword}: {filtered_data.shape[0]}")
    n = st.slider("Number of Top Elements to Display", 5, 50, 25)

    # Check if filtered_data is not empty before processing
    if not filtered_data.empty:
        columns = [
            "hard_skills",
            "tech_stack",
            "soft_skills",
            "industries",
            "company",
            "benefits",
        ]

        def frequency_counter(data, column):
            freq = count_frequency(data[column])
            df = pd.DataFrame(freq.most_common(n), columns=[column, "Frequency"])
            return df

        dfs = [frequency_counter(filtered_data, column) for column in columns]

        # Plotting bar charts with Altair
        def plot_bar_chart(df, x, y, title):
            chart = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X(
                        x, sort=alt.SortField(field="Frequency", order="descending")
                    ),
                    y=y,
                    tooltip=[x, y],  # Add tooltips
                )
                .properties(title=title, width="container", height=400)
                .interactive()
            )  # Make the chart interactive
            return chart

        def plot_altair_chart(df, column_name):
            title = column_name.replace("_", " ").title()

            if title == "Company":
                st.header(f"Companies Hiring Most for {keyword}")
            else:
                st.header(f"Top {title} for {keyword}")
            st.altair_chart(
                plot_bar_chart(
                    df,
                    f"{column_name}",
                    "Frequency",
                    f"Top {n} In-Demand {title} for {keyword}",
                ),
                use_container_width=True,
            )

        for i, df in enumerate(dfs):
            plot_altair_chart(df, columns[i])

    else:
        st.write("No data found for the given keyword.")

    oldest_date = filtered_data["time_extracted"].min()
    most_recent_date = filtered_data["time_extracted"].max()

    """
    ### Data Table with Company Filter
    """
    companies_list = [f"All Companies"] + dfs[-2]["company"].to_list() # Get the company list from the second to last dataframe in dfs
    company = st.selectbox(
        f"View all companies, or select one from the top {n} hiring most.",
        companies_list,
    )

    # Filter data based on the selected company
    if "All" in company:
        filtered_data = filtered_data
    else:
        filtered_data = filtered_data[
            filtered_data["company"].str.lower() == company.lower()
        ]

    st.write(
        "Tip: Adjust the slider above the bar charts to add more company options to the dropdown menu above!"
    )
    filtered_data

    with st.expander("Company Charts"):
        def get_freq_table(df, column_name, n):
            freq = count_frequency(df[column_name])
            df = pd.DataFrame(freq.most_common(n), columns=[column_name, "Frequency"])
            return df

        tech_stack_df = get_freq_table(filtered_data, "tech_stack", n)
        hard_skills_df = get_freq_table(filtered_data, "hard_skills", n)
        soft_skills_df = get_freq_table(filtered_data, "soft_skills", n)
        industries_df = get_freq_table(filtered_data, "industries", n)
        benefits_df = get_freq_table(filtered_data, "benefits", n)

        def plot_altair_chart(df, column_name):
            title = column_name.replace("_", " ").title()
            st.header(f"Top {title} for {keyword} at {company.title()}")
            st.altair_chart(
                plot_bar_chart(
                    df,
                    f"{column_name}",
                    "Frequency",
                    f"Top {n} In-Demand {title} for {keyword} at {company.title()}",
                ),
                use_container_width=True,
            )

        plot_altair_chart(tech_stack_df, "tech_stack")
        plot_altair_chart(hard_skills_df, "hard_skills")
        plot_altair_chart(soft_skills_df, "soft_skills")
        plot_altair_chart(industries_df, "industries")
        plot_altair_chart(benefits_df, "benefits")

        st.write(f"Oldest data pull: {oldest_date}")
        st.write(f"Recent data pull: {most_recent_date}")

with st.expander("Excluded Companies"):
    excluded_companies = ", ".join(sorted(blacklist_companies))
    st.write(
        """
        The following recruiting/consulting companies were excluded because 
        they hire on behalf of other companies and have a 
        disproportionately high number of job postings:
        """
    )
    st.write(excluded_companies)
