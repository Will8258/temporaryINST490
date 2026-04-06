import pandas as pd
import requests
import os
import io

# --------------------------------------------------------- #
# CONFIGURATION
# --------------------------------------------------------- #
DATASET_ID = "w3bc-8mnv"
BASE_URL = f"https://opendata.maryland.gov/resource/{DATASET_ID}.json"
OUTPUT_DIR = "maryland_foreclosure_data"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"[INFO] Fetching data from Maryland Open Data ({DATASET_ID})...")

# --------------------------------------------------------- #
# 1. FETCH DATA
# --------------------------------------------------------- #
# Socrata limits to 1000 rows by default, so we set a high limit to get everything
params = {"$limit": "50000"} 
response = requests.get(BASE_URL, params=params)

if response.status_code != 200:
    print(f"[ERROR] Failed to fetch data: {response.status_code}")
    exit()

# Load into DataFrame
df = pd.DataFrame(response.json())

print(f"[INFO] Raw data loaded. {len(df)} rows found.")
print(f"[INFO] Columns found: {list(df.columns)}")

# --------------------------------------------------------- #
# 2. DATA TRANSFORMATION
# --------------------------------------------------------- #
# The Socrata API returns data in a "Wide by County" format similar to:
# date, type, allegany_county, anne_arundel_county, ...

# Identify the columns
date_col = 'date'
type_col = 'type'

# Dynamically identify county columns (all columns that are not date or type)
# We exclude system columns if any (starting with :)
county_cols = [c for c in df.columns if c not in [date_col, type_col] and not c.startswith(":")]

print(f"[INFO] Processing {len(county_cols)} counties...")

# Step A: "Melt" the dataframe (Unpivot)
# Turn columns (Allegany, Anne Arundel) into rows
# Result: date | type | county_name | value
df_melted = df.melt(id_vars=[date_col, type_col], 
                    value_vars=county_cols, 
                    var_name='county_name', 
                    value_name='value')

# Convert value to numeric (handle errors if any non-numbers exist)
df_melted['value'] = pd.to_numeric(df_melted['value'], errors='coerce').fillna(0)

# Step B: "Pivot" the dataframe
# We want: Index=Date, Columns=Type (NOI, NOF, FPR), Values=Count
# But we need to do this *per county*.
grouped = df_melted.groupby('county_name')

# --------------------------------------------------------- #
# 3. EXPORT PER COUNTY
# --------------------------------------------------------- #
for county_clean_name, county_data in grouped:
    
    # Pivot: Dates as rows, Types as columns
    df_county_pivoted = county_data.pivot(index=date_col, columns=type_col, values='value')
    
    # Reset index to make Date a column again
    df_county_pivoted = df_county_pivoted.reset_index()
    
    # Rename 'date' to 'OBSERVATION DATE' to match client sheets
    df_county_pivoted = df_county_pivoted.rename(columns={date_col: 'OBSERVATION DATE'})
    
    # Sort by date
    df_county_pivoted = df_county_pivoted.sort_values('OBSERVATION DATE')
    
    # Clean up the Date format (remove T00:00:00.000)
    # Socrata dates look like '2021-07-01T00:00:00.000'. We want '2021-07-01'.
    df_county_pivoted['OBSERVATION DATE'] = df_county_pivoted['OBSERVATION DATE'].str.split('T').str[0]
    
    # Clean up column names (remove index name)
    df_county_pivoted.columns.name = None
    
    # Generate filename (Clean up county name format, e.g., 'allegany_county' -> 'ALLEGANY')
    filename_county = county_clean_name.replace("_county", "").upper()
    save_path = os.path.join(OUTPUT_DIR, f"{filename_county}.csv")
    
    # Save
    df_county_pivoted.to_csv(save_path, index=False)
    print(f"  [SAVED] {save_path}")

print("\n[INFO] Process Complete. Check the output folder!")