# The Bureau of Labor Statistics (BLS) API
# Created: 11/17/2025
# Last Updated: 12/03/2025 [MODIFIED]

import requests
import json
import prettytable
import csv 
from datetime import datetime
import pandas as pd
from fredapi import Fred
import os
import yaml
import re
import time
from urllib.error import HTTPError

# ------------------------------- #
# Access BLS API with unique key #
# ------------------------------- #

os.chdir(os.path.dirname(os.path.realpath(__file__)))
current_directory = os.getcwd()

with open("api_keys.yaml", "r") as f:
    keys = yaml.safe_load(f)

BLS_API_KEY = keys["bls_api"]

# ------------------------------------ #
# Create time buffers between requests #
# ------------------------------------ #

SLEEP_TIME = 0.5  
MAX_RETRIES = 3   

# ------------------------------- #
# Helper Function for File Naming #
# ------------------------------- #

def to_snake_case(s):
    """
    Convert a string to snake_case suitable for filenames:
    - lowercase
    - spaces and hyphens replaced with underscores
    - remove parentheses, slashes, colons, and other special characters
    - collapse multiple underscores
    """
    s = s.lower()
    s = re.sub(r"[ /\\\-]", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s

# ----------------------------------- #
# Helper Function for Data Formatting #
# ----------------------------------- #

month_lookup = {
    "M01": "January", "M02": "February", "M03": "March", "M04": "April",
    "M05": "May", "M06": "June", "M07": "July", "M08": "August",
    "M09": "September", "M10": "October", "M11": "November", "M12": "December"
}

def make_date(year, period):
    if period.startswith("M"):
        month = int(period[1:])
        return datetime(int(year), month, 1).strftime("%Y-%m-%d")
    return None

# -------------------------------- #
# Loading Data with Series ID Info #
# -------------------------------- #

file_path = "Indicators Series ID List.xlsx" 
county_sheet = "COUNTY BLS" 
county_series_id_df = pd.read_excel(file_path, sheet_name=f"{county_sheet}", skiprows=0)

new_col_names = ['COUNTY', 'SERIES ID']
county_series_id_df.columns = new_col_names

# =============================================================================
# [MODIFICATION START] - Expanding Metrics & Preparing for "Melt"
# Reason: We need Unemployment Rate, Count, and Labor Force, not just Employment.
# We also need to fix the missing Baltimore City ID.
# =============================================================================

# 1. Clean whitespace
county_series_id_df['COUNTY'] = county_series_id_df['COUNTY'].str.strip()

# 2. Fix Baltimore City
# [OLD CODE] (Was relying on the sheet which said "Check County FRED sheet")
# county_series_id_df.loc[county_series_id_df['COUNTY'] == 'Baltimore City', 'SERIES ID'] = "Check County FRED sheet"
# [NEW CODE] Manually insert the correct BLS ID for Baltimore City
county_series_id_df.loc[county_series_id_df['COUNTY'] == 'Baltimore City', 'SERIES ID'] = 'LAUCN245100000000005'

# 3. Generate IDs for the other metrics automatically
# BLS codes: 05=Employment, 04=Unemployment Count, 03=Unemployment Rate, 06=Labor Force
county_series_id_df['Unemployment Count ID'] = county_series_id_df['SERIES ID'].str.replace('05$', '04', regex=True)
county_series_id_df['Unemployment Rate ID']  = county_series_id_df['SERIES ID'].str.replace('05$', '03', regex=True)
county_series_id_df['Labor Force ID']        = county_series_id_df['SERIES ID'].str.replace('05$', '06', regex=True)

# 4. "Melt" the dataframe
# Transforms the table so every row is a single Series ID.
county_series_melted = county_series_id_df.melt(
    id_vars=['COUNTY'], 
    value_vars=['SERIES ID', 'Unemployment Count ID', 'Unemployment Rate ID', 'Labor Force ID'],
    var_name='Metric', 
    value_name='Series ID'
)

# Clean up Metric names
metric_map = {
    'SERIES ID': 'Employment',
    'Unemployment Count ID': 'Unemployment Count',
    'Unemployment Rate ID': 'Unemployment Rate',
    'Labor Force ID': 'Labor Force'
}
county_series_melted['Metric'] = county_series_melted['Metric'].map(metric_map)

print("--- Ready for API ---")
print(county_series_melted.head())
print(f"Total Series to fetch: {len(county_series_melted)}")

# =============================================================================
# [MODIFICATION END]
# =============================================================================


# ------------------------------------- #
# Setting Up Saving and File Management #
# ------------------------------------- #

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd()

separate_dir = os.path.join(
    script_dir,
    "bls_csv_outputs",
    "county_data",
    "separate"
)

os.makedirs(separate_dir, exist_ok=True)
print(f"[INFO] Saving files to: {separate_dir}")

# =============================================================================
# [MODIFICATION START] - Filename Mapping & Batched Request Loop
# Reason: The API fails if we send 96 IDs at once (Limit is ~25-50).
# We also want files named "allegany_employment.csv", not "LAUCN....csv".
# =============================================================================

# 1. Create a "Map" for renaming files to human-readable format
id_to_filename = {}
for index, row in county_series_melted.iterrows():
    clean_county = to_snake_case(row['COUNTY'])
    clean_metric = to_snake_case(row['Metric'])
    friendly_name = f"{clean_county}_{clean_metric}"
    id_to_filename[row['Series ID']] = friendly_name

# 2. Get the full list of IDs
all_series_ids = county_series_melted['Series ID'].unique().tolist()

# 3. Process in chunks
chunk_size = 25
print(f"[INFO] Starting download for {len(all_series_ids)} series in batches...")

# Loop through the IDs in batches
for i in range(0, len(all_series_ids), chunk_size):
    current_chunk = all_series_ids[i : i + chunk_size]
    print(f"  > Processing batch {i//chunk_size + 1} ({len(current_chunk)} IDs)...")

    headers = {'Content-type': 'application/json'}
    
    # Use the dynamic chunk of IDs
    data = {
        "seriesid": current_chunk, 
        "startyear": "2011",
        "endyear": "2014"
    }

    try:
        response = requests.post(
            'https://api.bls.gov/publicAPI/v2/timeseries/data/',
            data=json.dumps(data),
            headers=headers
        )
        json_data = response.json()
        
        if json_data.get('status') == 'REQUEST_NOT_PROCESSED':
            print(f"    [ERROR] Batch failed: {json_data.get('message')}")
            continue

    except Exception as e:
        print(f"    [ERROR] Request failed: {e}")
        continue

    # ---- Write CSVs for this chunk ----
    if 'Results' in json_data and 'series' in json_data['Results']:
        for series in json_data['Results']['series']:
            
            series_id = series['seriesID']
            
            # Use the dictionary to find the readable name
            filename_base = id_to_filename.get(series_id, series_id)
            filepath = os.path.join(separate_dir, f"{filename_base}.csv")

            with open(filepath, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["series_id", "year", "month", "date", "value", "footnotes"])

                for item in series["data"]:
                    year = item.get("year")
                    period = item["period"]

                    if period.startswith("M"):
                        month_name = month_lookup.get(period, period)
                        date_value = make_date(year, period)

                        footnotes = ",".join(
                            fn["text"] for fn in item.get("footnotes", []) if fn
                        )

                        writer.writerow([
                            series_id,
                            year,
                            month_name,
                            date_value,
                            item.get("value"),
                            footnotes
                        ])

            print(f"    [SAVED] {filename_base}.csv")
            
    # Be nice to the API
    time.sleep(SLEEP_TIME)

# Script completion confirmation statement
print("[INFO] All downloads complete!")
print("[INFO] Files saved in: bls_csv_outputs/county_data/separate")

# --------------------------------------------------------- #
# POST-PROCESSING: Merge 96 files into 24 County Files      #
# --------------------------------------------------------- #

print("\n[INFO] Starting merge process...")

# Dictionary to store dataframes: {'allegany': [df_emp, df_unemp, ...]}
county_dfs = {}

# 1. Loop through all CSVs in the output folder
for filename in os.listdir(separate_dir):
    if filename.endswith(".csv"):
        # filename example: "allegany_unemployment_rate.csv"
        
        # Split the filename to get the county and the metric
        # We assume the format is always "{county}_{metric}.csv"
        # This is a bit tricky since counties can have underscores (prince_georges)
        # So we look at your 'county_series_melted' map to be safe, 
        # OR we can just use string manipulation if we trust the naming convention.
        
        # Let's use a smarter way: parsing the filename
        # Remove .csv
        name_no_ext = filename.replace(".csv", "")
        
        # Identify the metric from the end of the string
        if name_no_ext.endswith("_employment"):
            county_name = name_no_ext.replace("_employment", "")
            metric_col = "Employment"
        elif name_no_ext.endswith("_unemployment_count"):
            county_name = name_no_ext.replace("_unemployment_count", "")
            metric_col = "Unemployment Count"
        elif name_no_ext.endswith("_unemployment_rate"):
            county_name = name_no_ext.replace("_unemployment_rate", "")
            metric_col = "Unemployment Rate"
        elif name_no_ext.endswith("_labor_force"):
            county_name = name_no_ext.replace("_labor_force", "")
            metric_col = "Labor Force"
        else:
            print(f"  [SKIP] Could not identify metric for: {filename}")
            continue
            
        # Read the CSV
        file_path = os.path.join(separate_dir, filename)
        df = pd.read_csv(file_path)
        
        # Keep only date and value
        df = df[['date', 'value']]
        
        # Rename 'value' to the specific metric (e.g., 'Unemployment Rate')
        df = df.rename(columns={'value': metric_col})
        
        # Add to our dictionary
        if county_name not in county_dfs:
            county_dfs[county_name] = []
        county_dfs[county_name].append(df)

# 2. Merge and Save
merged_output_dir = os.path.join(script_dir, "bls_csv_outputs", "county_data", "merged")
os.makedirs(merged_output_dir, exist_ok=True)

for county_name, df_list in county_dfs.items():
    if not df_list:
        continue
        
    # Start with the first dataframe in the list
    merged_df = df_list[0]
    
    # Merge the rest
    for next_df in df_list[1:]:
        merged_df = pd.merge(merged_df, next_df, on='date', how='outer')
        
    # Sort by date
    merged_df = merged_df.sort_values('date')
    
    # Save
    save_path = os.path.join(merged_output_dir, f"{county_name}_all_metrics.csv")
    merged_df.to_csv(save_path, index=False)
    print(f"  [MERGED] Saved {county_name}_all_metrics.csv")


# Script completion confirmation statement
print("[INFO] Merging process complete!")
print("[INFO] Merged files saved in: bls_csv_outputs/county_data/merged")
