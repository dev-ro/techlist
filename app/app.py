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

    @st.cache_data(ttl=3600) # Cache data for 1 hour
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
        "interpersonal skills": "interpersonal"
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

    # Blacklist of companies
    blacklist_companies = [
        "synergisticit",
        "intellectt inc",
        "clearancejobs",
        "steneral consulting",
        "syntricate technologies",
        "1872 consulting",
        "acs consultancy services, inc",
        "mygwork - lgbtq+ business community",
        "zortech solutions",
        "energy jobline",
        "kyyba inc",
        "motion recruitment",
        "jobs via efinancialcareers",
        "robert half",
        "accroid inc",
        "stellent it",
        "software technology inc.",
        "donato technologies, inc.",
        "tekintegral",
        "extend information systems inc.",
        "keylent inc",
        "kforce inc",
        "ampcus inc",
        "get it recruit - information technology",
        "cybercoders",
        "diverse lynx",
        "remoteworker us",
        "harnham",
        "augment jobs",
        "tata consulting company",
        "tata consultancy services",
        "clickjobs.io",
        "jobot",
        "TekWissen ®",
        "dice",
        "techtammina llc",
        "cynet systems",
        "iconma",
        "spectraforce",
        "agile tech labs",
        "genesis10",
        "insight global",
        "ceres group",
        "smartiplace",
        "jobs malaysia - two95 hr hub",
        "stellar professionals",
        "lancesoft, inc.",
        "divihn integration inc",
        "wise skulls",
        "cybertec, inc",
        "lorven technologies inc.",
        "georgia it, inc.",
        "avid technology professionals",
        "hcl global systems inc",
        "excel hire staffing,llc",
        "capgemini",
        "randstad usa",
        "v-soft consulting group, inc.",
        "mission technologies, a division of hii",
        "prohires",
        "roberts recruiting, llc",
        "caci international inc",
        "mantech",
        "belay technologies",
        "mindlance",
        "psrtek",
        "info way solutions",
        "the judge group",
        "ziprecruiter",
        "hexaquest global",
        "captivation",
        "conch technologies, inc",
        "open systems technologies",
        "acceler8 talent",
        "alldus",
        "clifyx",
        "marathon ts",
        "aptask",
        "v2soft",
        "hatchpros",
        "aditi consulting",
        "ltimindtree",
        "software people inc.",
        "lasalle network",
        "compunnel inc.",
        "guidehouse",
        "intersources inc",
        "ev.careers",
        "resource informatics group, inc",
        "htc global services",
        "pyramid consulting, inc",
        "artech l.l.c.",
        "axelon services corporation",
        "pi square technologies",
        "enexus global inc.",
        "algo capital group",
        "anveta, inc",
        "akraya, inc.",
        "softworld, a kelly company",
        "ascendion",
        "akkodis",
        "fasttek global",
        "system soft technologies",
        "sky consulting inc.",
        "intelliswift software",
        "qinetiq us (formerly avantus federal)",
        "lhh",
        "chelsoft solutions co.",
        "serigor inc",
        "us tech solutions",
        "inspyr solutions",
        "amtex systems inc.",
        "shiftcode analytics, inc.",
        "etek it services, inc.",
        "integrated resources, inc ( iri )",
        "eteam",
        "applab systems, inc",
        "selby jennings",
        "lmi",
        "cps, inc.",
        "mindpal",
        "usajobs",
        "caterpillar inc.",
        "systems technology group, inc. (stg)",
        "team remotely inc",
        "megan soft inc",
        "inficare staffing",
        "dcs corp",
        "hexaware technologies",
        "stanley reid",
        "epitec",
        "trispoke managed services pvt. ltd.",
        "actalent",
        "jesica.ai",
        "stealth startup",
        "paradyme, inc.",
        "qinetiq us",
    ]

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
    @st.cache_data
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
    @st.cache_data
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
        # Count frequency of words in each category
        hard_skills_freq = count_frequency(filtered_data["hard_skills"])
        tech_stack_freq = count_frequency(filtered_data["tech_stack"])
        soft_skills_freq = count_frequency(filtered_data["soft_skills"])
        industries_freq = count_frequency(filtered_data["industries"])
        companies_freq = count_frequency(filtered_data["company"])
        benefits_freq = count_frequency(filtered_data["benefits"])

        # Convert frequency counts to DataFrame for plotting and sort them
        hard_skills_df = pd.DataFrame(
            hard_skills_freq.most_common(n), columns=["Skill", "Frequency"]
        )
        tech_stack_df = pd.DataFrame(
            tech_stack_freq.most_common(n), columns=["Tech", "Frequency"]
        )
        soft_skills_df = pd.DataFrame(
            soft_skills_freq.most_common(n), columns=["Skill", "Frequency"]
        )
        industries_df = pd.DataFrame(
            industries_freq.most_common(n), columns=["Industry", "Frequency"]
        )
        companies_df = pd.DataFrame(
            companies_freq.most_common(n), columns=["Company", "Frequency"]
        )
        benefits_df = pd.DataFrame(
            benefits_freq.most_common(n), columns=["Benefit", "Frequency"]
        )

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

        st.header(f"Top Tech Stack for {keyword}")
        st.altair_chart(
            plot_bar_chart(
                tech_stack_df,
                "Tech",
                "Frequency",
                f"Top {n} In-Demand Tech Stack for {keyword}",
            ),
            use_container_width=True,
        )

        st.header(f"Top Hard Skills for {keyword}")
        st.altair_chart(
            plot_bar_chart(
                hard_skills_df,
                "Skill",
                "Frequency",
                f"Top {n} In-Demand Hard Skills for {keyword}",
            ),
            use_container_width=True,
        )

        st.header(f"Top Soft Skills for {keyword}")
        st.altair_chart(
            plot_bar_chart(
                soft_skills_df,
                "Skill",
                "Frequency",
                f"Top {n} In-Demand Soft Skills for {keyword}",
            ),
            use_container_width=True,
        )

        st.header(f"Top Industries for {keyword}")
        st.altair_chart(
            plot_bar_chart(
                industries_df,
                "Industry",
                "Frequency",
                f"Top {n} In-Demand Industries for {keyword}",
            ),
            use_container_width=True,
        )

        st.header(f"Companies Hiring for {keyword}")
        st.altair_chart(
            plot_bar_chart(
                companies_df,
                "Company",
                "Frequency",
                f"Companies Hiring Most for {keyword}",
            ),
            use_container_width=True,
        )

        st.header(f"Top Benefits for {keyword}")
        st.altair_chart(
            plot_bar_chart(
                benefits_df, "Benefit", "Frequency", f"Top {n} Benefits for {keyword}"
            ),
            use_container_width=True,
        )

    else:
        st.write("No data found for the given keyword.")

    oldest_date = filtered_data["time_extracted"].min()
    most_recent_date = filtered_data["time_extracted"].max()

    st.write(f"Oldest data pull: {oldest_date}")
    st.write(f"Recent data pull: {most_recent_date}")

    excluded_companies = ", ".join(sorted(blacklist_companies))

    st.write(
        f"The following companies have been excluded from the analysis: {excluded_companies}"
    )
    st.write(f"Number of excluded job postings: {excluded_jobs_count}")

    """
    ## Data Table
    """
    companies_list = [f"All Companies"] + companies_df['Company'].to_list()
    company = st.selectbox("View jobs by company. Select a company", companies_list)

    # Filter data based on the selected company
    if "All" in company:
        filtered_data = filtered_data
    else:
        filtered_data = filtered_data[filtered_data["company"].str.lower() == company.lower()]

    filtered_data
        