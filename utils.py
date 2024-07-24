def generate_blacklist_sql_query(companies):
    # Start of the SQL query
    query = "-- Insert data into the blacklist table\n"
    query += "INSERT INTO `extracted_data.blacklist` (company, added_on)\nVALUES\n"
    
    # Generate the VALUES part of the query
    values = []
    for company in companies:
        # Escape single quotes in company names to prevent SQL injection
        escaped_company = company.replace("'", "''")
        values.append(f"  ('{escaped_company}', CURRENT_TIMESTAMP())")
    
    # Join the values with commas and newlines
    query += ",\n".join(values)
    
    # Add the final semicolon
    query += ";"
    
    return query

    # Blacklist of companies
blacklist_companies = [
    "rit solutions, inc.",
    "adame services llc",
    "adame services llc",
    "executive staff recruiters / esr healthcare",
    "stefanini north america and apac",
    "ip recruiter group",
]
sql_query = generate_blacklist_sql_query(sorted(blacklist_companies))
print(sql_query)