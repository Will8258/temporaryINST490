# Federal Reserve Economic Data (FRED) API
# Created: 10/2025
# Last Updated: 11/17/2025 

# Things to install if haven't already (only once)
    # Install the Fred API "pip install fredapi"
    # Install openpyxl in order to open .xlsx files in pandas through "pip install openpyxl"
    # Install pyyaml in order to retrieve hidden API saved in a YAML file through "pip install PyYAML"

import pandas as pd  # Data cleaning
from fredapi import Fred  # Accessing data
import os  # File management (reading and saving)
import yaml  # Load API key from a YAML file for security purposes
import re  # For file naming manipulation
import time  # To buffer API requests
from urllib.error import HTTPError  # Handle API request limit

# -------------------------------- #
# Setting up for FRED API Requests #
# -------------------------------- #

# Create time buffers between API requests
SLEEP_TIME = 0.5  # seconds between requests
MAX_RETRIES = 3   # number of retries if rate limit hit

def safe_get_series_info(fred, series_id, retries=MAX_RETRIES):
    """Fetch series metadata with retries on rate limit errors."""
    for attempt in range(retries):
        try:
            return fred.get_series_info(series_id)
        except ValueError as e:
            if "Too Many Requests" in str(e):
                wait = 5 * (attempt + 1)  # Exponential backoff
                print(f"[WARNING] Rate limit hit for {series_id}. Waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                raise
    raise ValueError(f"Failed to fetch series {series_id} after {retries} attempts.")

def safe_get_series(fred, series_id, retries=MAX_RETRIES):
    """Fetch series data with retries on rate limit errors."""
    for attempt in range(retries):
        try:
            return fred.get_series(series_id)
        except ValueError as e:
            if "Too Many Requests" in str(e):
                wait = 5 * (attempt + 1)
                print(f"[WARNING] Rate limit hit for data {series_id}. Waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                raise
    raise ValueError(f"Failed to fetch series data {series_id} after {retries} attempts.")

# ------------------------------- #
# Helper Function for File Naming #
# ------------------------------- #

# Ensure snake_case and proper naming convention
def to_snake_case(s):
    """
    Convert a string to snake_case suitable for filenames:
    - lowercase
    - spaces and hyphens replaced with underscores
    - remove parentheses, slashes, colons, and other special characters
    - collapse multiple underscores
    """
    s = s.lower()                      # lowercase
    s = re.sub(r"[ /\\\-]", "_", s)    # replace space, /, \, - with _
    s = re.sub(r"[^a-z0-9_]", "", s)   # remove all non-alphanumeric and non-underscore chars
    s = re.sub(r"_+", "_", s)          # collapse multiple underscores
    s = s.strip("_")                    # remove leading/trailing underscores
    return s

# ------------------------------- #
# Access FRED API with unique key #
# ------------------------------- #
# Ensures that code is being run in the appropriate directory (which also contains the .yaml file)
os.chdir(os.path.dirname(os.path.realpath(__file__)))
current_directory = os.getcwd()
#print(f"The current working directory is: {current_directory}")

# Load API key from YAML -- key hidden for security reasons
with open("api_keys.yaml", "r") as f:
    keys = yaml.safe_load(f)

FRED_API_KEY = keys["fred_api"]

# Initialize FRED client
fred = Fred(api_key=FRED_API_KEY)

# -------------------------------- #
# Loading Data with Series ID Info #
# -------------------------------- #

# Load table with series IDs that is downloaded from Google Drive
file_path = "Indicators Series ID List.xlsx"  # Can be updated if Excel file changes

# County Series IDs
county_sheet = "COUNTY FRED"  # Can be updated if Excel file changes
    # Read  Excel file -- Note: Skipping first row, since column headings are merged in row 0
county_series_id_df = pd.read_excel(file_path, sheet_name=f"{county_sheet}", skiprows=1)

# Clean the COUNTY column (for any extra spaces) - must match FRED API County Names
county_series_id_df["COUNTY"] = county_series_id_df["COUNTY"].astype(str).str.strip()

# State of Maryland Series IDs
state_sheet = "MD FRED"  # Can be updated if Excel file changes
    # Read  Excel file
state_series_id_df = pd.read_excel(file_path, sheet_name=f"{state_sheet}")

# Preview County and State Series ID Data
#print(county_series_id_df.head())  # County Series IDs
#print(state_series_id_df.head())  # State of Maryland Series IDs

# ------------------------------------- #
# Setting Up Saving and File Management #
# ------------------------------------- #

# Save CSV outputs to an a specific output folder

# Base folder relative to script location (or absolute path if you prefer)
script_dir = os.path.dirname(os.path.abspath(__file__))

# Folder for county-level outputs
county_output_folder = os.path.join(script_dir, "fred_csv_outputs", "county_data")
os.makedirs(county_output_folder, exist_ok=True)
    # Base folder for county data
county_output_base = "fred_csv_outputs/county_data"

# Folder for state-level outputs
state_output_folder = os.path.join(script_dir, "fred_csv_outputs", "state_data")
os.makedirs(state_output_folder, exist_ok=True)
    # Base folder for state data
state_output_base = "fred_csv_outputs/state_data"

print(f"[INFO] County output folder created: {county_output_folder}")
print(f"[INFO] State output folder created: {state_output_folder}")


#################################################
# --------------------------------------------- #
# Fetching and Saving COUNTY Data from FRED API #
# --------------------------------------------- #
#################################################

# OPTIONAL: Practicing with Montgomery County, MD -- County Name: "Montgomery"
# Change to "county_series_id_df" later
#montgomery_df = county_series_id_df[county_series_id_df["COUNTY"] == "Montgomery"]

# Iterate through counties and series
for idx, row in county_series_id_df.iterrows():
    county = row["COUNTY"]

    # Create folder for this county
    county_folder = os.path.join(county_output_base, to_snake_case(county))
    os.makedirs(county_folder, exist_ok=True)  # create folder if it doesn't exist

    for col in county_series_id_df.columns[1:]:
        series_id = row[col]
        if pd.isna(series_id) or series_id == "":  # Skips blanks
            continue

        # Prepare file path
        county_snake_case = to_snake_case(county)
        col_snake_case = to_snake_case(col)
        out_path = os.path.join(county_folder, f"{county_snake_case}_{col_snake_case}.csv")

        # -------------------------------------------------------
        # LEAVE THIS SECTION COMMENTED OUT (unless special case)
        # -------------------------------------------------------

        # This will will make it so it executres calls for data that hasn't already been saved
            # Can be helpful if going over request limit
            # Skips calling for data if it was previously downloaded
            # Caution: It is wise to keep this commented-out so old downloaded data can be overwritten
        #if os.path.exists(out_path):
        #    print(f"[INFO] File already exists, skipping: {os.path.relpath(out_path)}\n")
        #    continue

        # ---------------------
        # Fetch metadata
        # ---------------------
        
        # Sets default values to None
        title = source = freq = obs_start = obs_end = None

        try:
            meta = fred.get_series_info(series_id)
        except Exception as e:
            print(f"[WARN] Could not fetch metadata for {series_id}: {e}. Skipping metadata.")
        
        calls = ["title", "source_name", "frequency_short", "observation_start", "observation_end"]
        metadata = []
    
        for element in calls:
            
            x = None  # Default is None

            try:
                x = meta.get(element, None)
            except Exception as e:
                print(f"[WARN] Could not fetch metadata for {element}: {e}. Skipping metadata.")

            metadata.append(x)

        title = metadata[0]  # Data from calls[0]
        source = metadata[1]
        freq = metadata[2]
        obs_start = metadata[3]
        obs_end = metadata[4]

        # Print metadata confirmation
        print(f"County: {county} | Series: {col} ({series_id})")
        print(f"Title: {title}")
        print(f"Source: {source}")
        print(f"Frequency: {freq}")
        print(f"Observation Start: {obs_start}")
        print(f"Observation End: {obs_end}")
        print("-"*50)

        # -------------------------
        # Fetch series with exponential backoff
        # -------------------------
        retries = 0
        max_retries = 8
        wait_seconds = 10  # Initial wait time for exponential backoff

        while retries < max_retries:
            try:
                data = fred.get_series(series_id)
                break  # Success, exit retry loop
            except HTTPError as e:
                if e.code == 429:
                    retries += 1
                    print(f"[WARN] Rate limit hit for {series_id}. Waiting {wait_seconds} seconds before retry {retries}/{max_retries}...")
                    time.sleep(wait_seconds)
                    wait_seconds *= 2  # Exponential backoff
                else:
                    print(f"[ERROR] HTTPError for {series_id}: {e}. Skipping.")
                    data = None
                    break
            except Exception as e:
                print(f"[ERROR] Unexpected error for {series_id}: {e}. Skipping.")
                data = None
                break

        if data is None:
            print(f"[ERROR] Could not fetch series {series_id}. Skipping.\n")
            continue

        # -------------------------
        # Rename and Save Data
        # -------------------------
        county_snake_case = to_snake_case(county)
        col_snake_case = to_snake_case(col)
        out_path = os.path.join(county_folder,f"{county_snake_case}_{col_snake_case}.csv")
        
        # Convert Series to DataFrame with generic column names
            # X-axis label
        df = data.to_frame(name="value")
            # Y-axis label
        df.index.name = "date"
        
        df.to_csv(out_path, header=True)
        relative_path = os.path.relpath(out_path, start=os.getcwd())
        print(f"[INFO] File saved to:\n{relative_path}\n")

         # Short sleep to spread requests and reduce hitting rate limit
        time.sleep(0.75)  # Could change to anywhere from 0.25 or 0.75


#################################################
# --------------------------------------------- #
# Fetching and Saving STATE Data from FRED API #
# --------------------------------------------- #
#################################################

# ------------------------- #
# STATE Data from FRED API #
# ------------------------- #

# Iterate through state series
for idx, row in state_series_id_df.iterrows():
    series_name = row["DATA TYPE"]
    series_id = row["SERIES ID"]

    if pd.isna(series_id) or series_id == "":
        continue

    # Fetch metadata
    try:
        meta = fred.get_series_info(series_id)
        title = meta.get("title", None)
        source = meta.get("source_name", None)
        freq = meta.get("frequency_short", None)
        obs_start = meta.get("observation_start", None)
        obs_end = meta.get("observation_end", None)
    except Exception as e:
        print(f"[WARN] Could not fetch metadata for {series_id}: {e}. Skipping metadata.")
        title = source = freq = obs_start = obs_end = None

    # Print metadata confirmation
    print(f"Series: {series_name} ({series_id})")
    print(f"Title: {title}")
    print(f"Source: {source}")
    print(f"Frequency: {freq}")
    print(f"Observation Start: {obs_start}")
    print(f"Observation End: {obs_end}")
    print("-"*50)

    # Fetch series data
    try:
        data = fred.get_series(series_id)
    except Exception as e:
        print(f"[ERROR] Could not fetch series {series_id}: {e}. Skipping.\n")
        continue

    # Save to CSV
    series_snake_case = to_snake_case(series_name)
    out_path = os.path.join(state_output_folder, f"{series_snake_case}.csv")
    
    # Convert Series to DataFrame with generic column names
        # X-axis label
    df = data.to_frame(name="value")
        # Y-axis label
    df.index.name = "date"
    
    df.to_csv(out_path, header=True)

    # Print relative path confirmation
    relative_path = os.path.relpath(out_path, start=os.getcwd())

    print(f"[INFO] File saved to:\n{relative_path}\n")
