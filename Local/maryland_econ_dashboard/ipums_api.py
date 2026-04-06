# IPUMS NHGIS Automation & Cleaning for INST490
# Updated: 12/2025

import os
import time
import json
import requests
import yaml
import pandas as pd
import zipfile
import shutil
from collections import defaultdict

# ------------------------- #
# 0. Setup & Configuration  #
# ------------------------- #
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Define Folders
md_demog_output_folder = os.path.join(script_dir, "ipums_csv_outputs", "md_demog")
raw_download_folder = os.path.join(script_dir, "ipums_csv_outputs", "raw_zips")
os.makedirs(md_demog_output_folder, exist_ok=True)
os.makedirs(raw_download_folder, exist_ok=True)

# --- CLEANUP: Wipe OLD files (Outputs AND Raw Zips) ---
print(f"[INFO] Cleaning up old files...")
for folder in [md_demog_output_folder, raw_download_folder]:
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")
print(f"[INFO] Folders wiped clean.")

# Load API Keys
with open("api_keys.yaml", "r") as f:
    keys = yaml.safe_load(f)

IPUMS_API_KEY = keys["ipums_api"].strip()
headers = {
    "Authorization": IPUMS_API_KEY,
    "Content-Type": "application/json"
}

# ------------------------- #
# 1. Load Excel Series IDs  #
# ------------------------- #
file_path = "Indicators Series ID List.xlsx"
state_sheet = "MD IPUMS NHGIS"
if not os.path.exists(file_path):
    print(f"[ERROR] Could not find {file_path}")
    exit()

series_id_df = pd.read_excel(file_path, sheet_name=state_sheet, skiprows=1)
id_list = series_id_df.iloc[0].dropna().astype(str).tolist()[1:]

# ------------------------- #
# 2. Parse IDs              #
# ------------------------- #
def split_id(x):
    name = x[:3]       # e.g., A00
    specifier = x[3:5] # e.g., AA
    year = x[-4:]      # e.g., 1920
    return pd.Series([name, specifier, year])

parsed_df = pd.DataFrame(id_list, columns=["full_id"])
parsed_df[["name", "specifier", "year"]] = parsed_df["full_id"].apply(split_id)
parsed_df["year"] = parsed_df["year"].astype(int)

# ------------------------- #
# 3. Build & Submit Extract #
# ------------------------- #
time_series_tables = defaultdict(lambda: {"years": set(), "geogLevels": ["state"]})
# NOTE: If you need county-level data, change ["state"] to ["county"] below
for _, row in parsed_df.iterrows():
    time_series_tables[row["name"]]["years"].add(str(row["year"]))

time_series_tables = {
    table: {"years": sorted(list(values["years"])), "geogLevels": values["geogLevels"]}
    for table, values in time_series_tables.items()
}

extract_payload = {
    "timeSeriesTables": time_series_tables,
    "description": "Maryland NHGIS extract: INST490",
    "dataFormat": "csv_header",
    "breakdownAndDataTypeLayout": "single_file",
    "timeSeriesTableLayout": "time_by_row_layout"
}

url = "https://api.ipums.org/extracts/?collection=nhgis&version=2"
response = requests.post(url, headers=headers, json=extract_payload)
response_json = response.json()

if "number" not in response_json:
    print("[ERROR] Submission failed.")
    print(response_json)
    exit()

extract_number = response_json["number"]
print(f"[INFO] Extract #{extract_number} submitted. Waiting for processing...")

# ------------------------- #
# 4. Wait & Download Logic  #
# ------------------------- #
status_url = f"https://api.ipums.org/extracts/{extract_number}?collection=nhgis&version=2"

while True:
    resp = requests.get(status_url, headers=headers)
    extract_info = resp.json()
    status = extract_info.get("status")
    
    if status == "completed":
        print("[INFO] Extract completed! Downloading...")
        break
    elif status == "failed":
        print("[ERROR] Extract generation failed.")
        exit()
    else:
        print(f"Status: {status}. Retrying in 30s...")
        time.sleep(30)

if "downloadLinks" in extract_info:
    download_url = extract_info["downloadLinks"]["tableData"]["url"]
    r = requests.get(download_url, headers=headers)
    zip_path = os.path.join(raw_download_folder, f"nhgis{extract_number:04d}.zip")
    with open(zip_path, "wb") as f:
        f.write(r.content)
else:
    print("[ERROR] Download link not found.")
    exit()

# Extract CSV
target_csv_name = ""
with zipfile.ZipFile(zip_path, 'r') as z:
    for filename in z.namelist():
        if filename.endswith(".csv") and "codebook" not in filename:
            z.extract(filename, raw_download_folder)
            target_csv_name = os.path.join(raw_download_folder, filename)
            print(f"[INFO] Extracted raw CSV: {target_csv_name}")

# ------------------------- #
# 5. Clean, Filter & Split  #
# ------------------------- #
print("-" * 50)
print("[INFO] Starting Data Cleaning...")

# --- VERIFIED FILENAME MAP (From Excel) ---
filename_map = {
    "A00": "Total_Population",
    "A08": "Total_Population_By_Sex",
    "D08": "Total_Population_By_Sex",
    "AR9": "Median_Age_Of_Persons",
    "B58": "Persons_By_Sex_By_Age",
    "B18": "Persons_By_Race",
    "AS9": "Persons_In_Housesholds_By_Sex",
    "A39": "Persons_In_Households_By_Age",
    "CQ9": "Persons_Under_18_In_Households",
    "CR1": "Persons_65_Years_And_Over_In_Households",
    "CR3": "Persons_65_Years_And_Over_In_The_Households_By_Household_Type",
    "A68": "Total_Families",
    "CL5": "Persons_In_Families",
    "AG4": "Families_By_Family_Type_By_Presence_And_Age_Of_Own_Children",
}

# Step A: Read Headers
df_raw = pd.read_csv(target_csv_name, header=None, low_memory=False)
header_codes = df_raw.iloc[0] 
header_names = df_raw.iloc[1] 
data_rows = df_raw.iloc[2:]   

data_rows.columns = header_codes

# Step B: Filter for Maryland
if "STATE" in data_rows.columns:
    md_data = data_rows[data_rows["STATE"] == "Maryland"].copy()
else:
    md_data = data_rows[data_rows.iloc[:, 2] == "Maryland"].copy() 

print(f"[INFO] Filtered rows for Maryland: {len(md_data)}")

# Step C: Base Columns
base_cols = ["GISJOIN", "YEAR", "STATE", "STATEFP", "STATENH", "NAME"]
valid_base_cols = [c for c in base_cols if c in md_data.columns]

# Step D: Group & Save
metric_cols = [c for c in md_data.columns if c not in valid_base_cols]

grouped_metrics = defaultdict(list)
for col in metric_cols:
    prefix = col[:3]
    grouped_metrics[prefix].append(col)

for prefix, cols in grouped_metrics.items():
    human_name = filename_map.get(prefix, f"Table_{prefix}")
    
    # Slice Data
    selection_cols = valid_base_cols + cols
    subset_data = md_data[selection_cols]
    
    # Headers
    header_df = pd.concat([header_codes.to_frame().T, header_names.to_frame().T])
    header_df.columns = header_codes 
    subset_headers = header_df[selection_cols]
    
    final_output = pd.concat([subset_headers, subset_data], ignore_index=True)
    
    # SAVE: Added {prefix} to filename to prevent duplicates
    out_filename = f"Maryland_{human_name}_{prefix}.csv"
    out_path = os.path.join(md_demog_output_folder, out_filename)
    final_output.to_csv(out_path, index=False, header=False) 
    
    print(f"   -> Saved {out_filename}")

print("-" * 50)
print(f"[SUCCESS] Process complete. Files saved in: {md_demog_output_folder}")