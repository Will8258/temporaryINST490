#!/usr/bin/env python
# coding: utf-8

# ## Using Plotly and Dash to Make Interactive Dashboards

# **_Used for Maryland County-Level Data_**
# 
# ---

# ### Installations and Upgrades
# Only have to install 1 time.
# If these have been installed/updated, you may need to restart the kernel.
# 
# * `openpxyl` = in order to open .xlsx files in pandas
# * `PyYAML` = in order to retrieve hidden API saved in a YAML file
# * `fredapi` = in order to access FRED API
# * `plotly` = in order to make interactive graphs
# * `dash` = in order to make interactive dashboards using plotly
# 
# If you’re only using these packages inside Jupyter Notebook, the following warning is harmless.
# The notebook uses the Python environment directly, so it doesn’t need the PATH to find the executables.
# 
# ```
# WARNING: The script flask is installed in '/home/lilgates/.local/bin' which is not on PATH.
#   Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
# 
# WARNING: The scripts dash-generate-components, dash-update-components, plotly and renderer are installed in '/home/lilgates/.local/bin' which is not on PATH.
# 
# Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location."
#   ```

# In[1]:


# Install all necessary packages
# If it is already installed, it will skip the installation
get_ipython().system('pip install openpyxl PyYAML fredapi dash plotly')


# In[2]:


# Upgrade Dash
get_ipython().system('pip install --upgrade dash')
get_ipython().system('pip install --upgrade typing_extensions')


# ### Imports
# Must import each time

# In[3]:


import pandas as pd  # Data cleaning
from fredapi import Fred  # Accessing data
import os  # File management (reading and saving)
import yaml  # Load API key from a YAML file for security purposes
import re  # For file naming manipulation
import time  # To buffer API requests
from urllib.error import HTTPError  # Handle API request limit
from dash import Dash, html, dcc, callback, Output, Input  # To make interactive dashboards
import plotly.express as px  # To make interactive graphs

from pathlib import Path  # To search through files/documents


# In[4]:


# Ensure Current Working Directory is correct

print("Current Working Directory:", os.getcwd())

# Base directories
state_base_dir = Path("csv_outputs/state_data")
county_base_dir = Path("csv_outputs/county_data")

# Test existence
print("State-Level:", state_base_dir.exists(), state_base_dir.is_dir())
print("County-Level:", county_base_dir.exists(), county_base_dir.is_dir())


# In[5]:


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


# In[6]:


# Organizing list of counties
county_list = county_series_id_df["COUNTY"].unique().tolist()

# Convert to snake_case using the function
county_list_snake = [to_snake_case(c) for c in county_list]

print("Original counties:")
print(county_list)
print("-"*60)
print("Snake-case counties:")
print(county_list_snake)


# In[ ]:





# In[7]:


# ----------------------------------------------- #
# Create set to store unique COUNTY-LEVEL metrics #
# ----------------------------------------------- #
county_metric_set = set()

for file in county_base_dir.rglob("*.csv"):
    folder_lower = file.parent.name.lower()
    
    # Only include counties in your list
    if folder_lower in county_list_snake:
        # Get filename without extension
        name_only = file.stem  # e.g., "baltimore_city_resident_population"
        
        # Ensure we only remove the **exact folder name prefix**
        prefix = folder_lower + "_"
        if name_only.startswith(prefix):
            metric = name_only[len(prefix):]  # remove the prefix safely
            county_metric_set.add(metric)

# Convert to a sorted list
county_metric_list = sorted(list(county_metric_set))

# ---------------------------------------------------------- #
# Compiling All COUNTY-LEVEL Metrics Sorted by Subject Group #
# ---------------------------------------------------------- #

# --- Define regex patterns for groups ---
group_county_patterns = {
    "housing": re.compile(r"house|housing", re.IGNORECASE),
    "labor": re.compile(r"employ|labor|labour", re.IGNORECASE),
    "economy": re.compile(r"poverty|gdp|population", re.IGNORECASE)
}

# --- Assign metrics to groups ---
grouped_county_metrics = {group: [] for group in group_county_patterns.keys()}

for metric in county_metric_list:
    for group, pattern in group_county_patterns.items():  # corrected
        if pattern.search(metric):
            grouped_county_metrics[group].append(metric)
            break

# --- Convert to a long-form DataFrame ---
rows = []
for group, metrics in grouped_county_metrics.items():
    for metric in metrics:
        rows.append({"group": group, "metric": metric})

county_metrics_df = pd.DataFrame(rows)

# --- Output ---
print("Metrics List:")
print(county_metric_list)

print("-"*60)

print("Metrics DataFrame (grouped):")
print(county_metrics_df)


# In[8]:


# ----------------------------------------------- #
# Create set to store unique STATE-LEVEL metrics #
# ----------------------------------------------- #
state_metric_set = set()

for file in state_base_dir.rglob("*.csv"):
    # Get filename without extension
    metric = file.stem  # e.g., "resident_population"
    state_metric_set.add(metric)

# Convert to a sorted list
state_metric_list = sorted(list(state_metric_set))
state_metric_list


# In[9]:


# ---------------------------------------------------------- #
# Compiling All STATE-LEVEL Metrics Sorted by Subject Group #
# ---------------------------------------------------------- #

# --- Define regex patterns for groups ---
group_state_patterns = {
    "housing": re.compile(r"house|housing|zillow", re.IGNORECASE),
    "labor": re.compile(r"employ|labor|labour|workers|unemployed", re.IGNORECASE),
    "economy": re.compile(r"poverty|gdp|population|income|business", re.IGNORECASE)
}


# --- Assign metrics to groups ---
grouped_state_metrics = {group: [] for group in group_state_patterns.keys()}

for metric in state_metric_list:
    for group, pattern in group_state_patterns.items():
        if pattern.search(metric):
            grouped_state_metrics[group].append(metric)
            break

# --- Convert to a long-form DataFrame ---
rows = []
for group, metrics in grouped_state_metrics.items():
    for metric in metrics:
        rows.append({"group": group, "metric": metric})

state_metrics_df = pd.DataFrame(rows)

# --- Output ---
print("State Metrics List:")
print(state_metric_list)

print("-"*60)

print("State Metrics DataFrame (grouped):")
print(state_metrics_df)


# In[10]:


# Dictionary to store STATE-LEVEL files by group

state_group_file_dict = {}

# Loop over each group in metrics_df
for group in ["housing", "labor", "economy"]:
    # List of metrics in this group
    metrics_in_group = county_metrics_df[county_metrics_df['group'] == group]['metric'].tolist()
    
    matching_files = []
    for file in state_base_dir.rglob("*.csv"):
        file_name = file.stem.lower()
        # Check if any metric in this group is in the filename
        if any(metric.lower() in file_name for metric in metrics_in_group):
            matching_files.append(file)
    
    state_group_file_dict[group] = matching_files

# --- Output ---
for group, files in state_group_file_dict.items():
    print(f"\nGroup: {group}")
    for f in files:
        print(f)


# In[22]:


# Dictionary to store COUNTY-LEVEL files by group

# Initialize nested dictionary
county_group_file_dict = {}

# Loop through all counties in county_list_snake
for county in county_list_snake:
    county_dir = county_base_dir / county
    if not county_dir.exists():
        continue  # skip if folder doesn't exist

    # Filter metrics by group
    for group in ["housing", "labor", "economy"]:
        # Get metrics in this group
        metrics_in_group = state_metrics_df[state_metrics_df['group'] == group]['metric'].tolist()
        
        # Find all CSVs in the county folder that match metrics in this group
        matching_files = []
        for file in county_dir.rglob("*.csv"):
            file_name = file.stem.lower()
            # Remove county prefix if present
            prefix = county + "_"
            if file_name.startswith(prefix):
                metric_name = file_name[len(prefix):]
            else:
                metric_name = file_name
            if any(metric.lower() == metric_name for metric in metrics_in_group):
                matching_files.append(file)
        
        # Add to nested dictionary
        county_group_file_dict.setdefault(county, {})[group] = matching_files

# --- Example output ---
for county, groups in county_group_file_dict.items():
    print(f"\nCounty: {county}")
    for group, files in groups.items():
        print(f"  Group: {group}")
        for f in files:
            print(f"    {f}")


# In[23]:


# ============================================
# ECONOMY PLOTLY – COUNTY-LEVEL GRAPHS
# ============================================

from pathlib import Path
import re
import pandas as pd
import plotly.express as px

# Use the same county_base_dir as before
county_base_dir = Path("csv_outputs/county_data")

# Helper: snake_case (same as before)
def to_snake_case(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[ /\\\-]", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")

# -------------------------------------------
# 1. ECONOMY METRIC STEMS (auto-detected)
# -------------------------------------------
# These keywords match your existing economy CSV filenames
ECONOMY_KEYWORDS = [
    "gdp",
    "real_gdp",
    "income",
    "wage",
    "wages",
    "earnings",
    "population",
    "resident_population",
    "poverty",
    "establishments",
    "business",
]

def detect_economy_metrics(county_snake):
    """
    Detect economy metric CSVs for the county by scanning filenames.
    """
    county_folder = county_base_dir / county_snake
    if not county_folder.exists():
        return []

    economy_files = []
    for path in county_folder.glob("*.csv"):
        stem = path.stem.lower()
        for kw in ECONOMY_KEYWORDS:
            if kw in stem:
                metric_key = stem[len(county_snake)+1:] if stem.startswith(county_snake+"_") else stem
                label = metric_key.replace("_", " ").title()
                economy_files.append({
                    "key": metric_key,
                    "label": label,
                    "path": path
                })
                break  # prevent duplicates from multiple keyword matches

    return sorted(economy_files, key=lambda x: x["label"])


# -------------------------------------------
# 2. Load ALL economy metrics for a county
# -------------------------------------------
def get_economy_data_for_county(county_name_pretty):
    county_snake = to_snake_case(county_name_pretty)
    files = detect_economy_metrics(county_snake)

    if not files:
        raise ValueError(f"No economy metrics found for county: {county_name_pretty}")

    frames = []
    for f in files:
        df = pd.read_csv(f["path"])
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        df["metric"] = f["label"]
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


# -------------------------------------------
# 3. Plotly chart for all economy metrics
# -------------------------------------------
def plot_economy_for_county(county_name_pretty):
    df = get_economy_data_for_county(county_name_pretty)

    fig = px.line(
        df,
        x="date",
        y="value",
        color="metric",
        title=f"Economy Indicators Over Time – {county_name_pretty}",
        markers=True,
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Value",
    )
    fig.show()


# In[24]:


plot_economy_for_county("Allegany")


# In[25]:


from ipywidgets import interact, Dropdown

def list_known_counties():
    """
    Look at folder names under county_base_dir and try to build
    'pretty' names for the dropdown.
    """
    counties = []
    for p in county_base_dir.iterdir():
        if p.is_dir():
            # reverse of to_snake_case: 'prince_georges_county_md' -> 'Prince Georges County Md'
            pretty = p.name.replace("_", " ").title()
            counties.append(pretty)
    return sorted(counties)

county_options = list_known_counties()

@interact(county=Dropdown(options=county_options, description="County:"))
def show_economy(county):
    plot_economy_for_county(county)


# In[26]:


def list_county_folders():
    return [p for p in county_base_dir.iterdir() if p.is_dir()]

def folder_to_pretty_name(folder_name: str) -> str:
    return folder_name.replace("_", " ").title()

def load_metric_all_counties(metric_substring: str):
    """
    metric_substring: e.g. "gdp", "income", "wage"
    Looks through each county folder, finds the first CSV whose stem
    contains that substring, and loads it.
    """
    frames = []

    for county_folder in list_county_folders():
        county_snake = county_folder.name
        county_pretty = folder_to_pretty_name(county_snake)

        csv_paths = list(county_folder.glob("*.csv"))
        matched = None
        for path in csv_paths:
            stem = path.stem.lower()
            if metric_substring.lower() in stem:
                matched = path
                break

        if matched is None:
            continue  # this county doesn’t have that metric

        df = pd.read_csv(matched)
        # Normalize date
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        # assume economy value column is named 'value'
        df["county"] = county_pretty
        frames.append(df[["date", "value", "county"]])

    if not frames:
        raise ValueError(f"No counties found with metric containing '{metric_substring}'")

    return pd.concat(frames, ignore_index=True)


# In[27]:


def plot_metric_all_counties(metric_substring: str, title=None):
    df = load_metric_all_counties(metric_substring)

    if title is None:
        title = f"Economy Metric – '{metric_substring}' Across Counties"

    fig = px.line(
        df,
        x="date",
        y="value",
        color="county",
        title=title,
        markers=False,
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Value",
        legend_title="County",
    )
    fig.show()


# In[28]:


plot_metric_all_counties("estimated_poverty_all_ages_percentage")
plot_metric_all_counties("real_gdp_all_industry_total_dollars")
plot_metric_all_counties("resident_population_thousands_of_persons")


# In[29]:


import plotly.graph_objects as go
from plotly.subplots import make_subplots


# In[30]:


def plot_economy_for_county_separate(county_name_pretty):
    # reuse your loader
    df = get_economy_data_for_county(county_name_pretty)

    metrics = sorted(df["metric"].unique())
    n_rows = len(metrics)

    # Create a subplot row for each metric
    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        subplot_titles=metrics,
        vertical_spacing=0.06,
    )

    for i, m in enumerate(metrics, start=1):
        dfm = df[df["metric"] == m]

        fig.add_trace(
            go.Scatter(
                x=dfm["date"],
                y=dfm["value"],
                mode="lines+markers",
                name=m,
                showlegend=False,   # legend not needed; title shows metric
            ),
            row=i,
            col=1,
        )

        fig.update_yaxes(title_text="Value", row=i, col=1)

    fig.update_xaxes(title_text="Date", row=n_rows, col=1)

    fig.update_layout(
        height=250 * n_rows,  # adjust if you want taller/shorter charts
        title_text=f"Economy Indicators Over Time – {county_name_pretty}",
    )

    fig.show()


# In[31]:


def plot_economy_for_county_separate(county_name_pretty):
    df = get_economy_data_for_county(county_name_pretty)

    metrics = sorted(df["metric"].unique())
    n_rows = len(metrics)

    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        subplot_titles=[m.replace("_", " ").title() for m in metrics],
        vertical_spacing=0.06,
    )

    for i, m in enumerate(metrics, start=1):
        df_m = df[df["metric"] == m].copy()

        # Clean name for hover
        pretty_metric = m.replace("_", " ").title()

        # Build hover template
        hover_template = (
            f"<b>{pretty_metric}</b><br>" +
            "Date: %{x|%Y-%m-%d}<br>" +
            "Value: %{y:,.2f}<extra></extra>"
        )

        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["value"],
                mode="lines+markers",
                name=pretty_metric,
                hovertemplate=hover_template,
                showlegend=False,
            ),
            row=i, col=1
        )

        fig.update_yaxes(title_text="Value", row=i, col=1)

    fig.update_xaxes(title_text="Date", row=n_rows, col=1)

    fig.update_layout(
        height=250 * n_rows,
        title_text=f"Economy Indicators Over Time – {county_name_pretty}",
    )

    fig.show()


# In[32]:


from ipywidgets import interact, Dropdown

def list_known_counties():
    counties = []
    for p in county_base_dir.iterdir():
        if p.is_dir():
            pretty = p.name.replace("_", " ").title()
            counties.append(pretty)
    return sorted(counties)

county_options = list_known_counties()

@interact(county=Dropdown(options=county_options, description="County:"))
def show_economy(county):
    plot_economy_for_county_separate(county)


# In[33]:


# ---------------------------------------------------
# LOAD LABOR DATA FOR A COUNTY (mirrors economy logic)
# ---------------------------------------------------

LABOR_KEYWORDS = [
    "all_employees_nonfarm",
    "civilian_labor_force_persons",
    "unemployed_persons_count",
    "unemployed_rate_percentage",
]

def detect_labor_metrics(county_snake):
    """
    Scan the county folder for labor metric CSVs.
    """
    county_folder = county_base_dir / county_snake
    if not county_folder.exists():
        return []

    labor_files = []
    for path in county_folder.glob("*.csv"):
        stem = path.stem.lower()
        for kw in LABOR_KEYWORDS:
            if kw in stem:
                label = kw.replace("_", " ").title()
                labor_files.append({
                    "key": kw,
                    "label": label,
                    "path": path
                })
                break

    return sorted(labor_files, key=lambda x: x["label"])


def get_labor_data_for_county(county_name_pretty):
    """
    Load and combine all labor CSVs for a county.
    Returns a dataframe with: date, value, metric
    """
    county_snake = to_snake_case(county_name_pretty)
    files = detect_labor_metrics(county_snake)

    if not files:
        raise ValueError(f"No labor metrics found for county: {county_name_pretty}")

    frames = []
    for f in files:
        df = pd.read_csv(f["path"])
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        df["metric"] = f["key"]      # raw metric key
        frames.append(df[["date", "value", "metric"]])

    return pd.concat(frames, ignore_index=True)


# In[34]:


import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_labor_for_county_separate(county_name_pretty):
    # Load your labor data (same pattern as economy)
    df = get_labor_data_for_county(county_name_pretty)

    # Sort metrics alphabetically so the order is consistent
    metrics = sorted(df["metric"].unique())
    n_rows = len(metrics)

    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        subplot_titles=[m.replace("_", " ").title() for m in metrics],
        vertical_spacing=0.06,
    )

    for i, m in enumerate(metrics, start=1):
        df_m = df[df["metric"] == m]

        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["value"],
                mode="lines+markers",
                name=m,
                showlegend=False
            ),
            row=i, col=1
        )

        fig.update_yaxes(title_text="Value", row=i, col=1)

    fig.update_xaxes(title_text="Date", row=n_rows, col=1)

    fig.update_layout(
        height=250 * n_rows,
        title_text=f"Labor Indicators Over Time – {county_name_pretty}",
    )

    fig.show()


# In[35]:


from ipywidgets import interact, Dropdown

@interact(county=Dropdown(options=list_known_counties(), description="County:"))
def show_labor(county):
    plot_labor_for_county_separate(county)


# In[36]:


from pathlib import Path
import pandas as pd
import plotly.express as px

# Base directory (you already have this in the notebook)
county_base_dir = Path("csv_outputs/county_data")

# --------------------------------------------
# HOUSING – CONFIG
# --------------------------------------------

housing_metrics = {
    # 1) Active listing count
    "active_listing_count": {
        "file": county_base_dir / "housing_active_listing_count.csv",
        "value_col": "active_listing_count",
        "title": "Housing Inventory: Active Listing Count by County",
        "yaxis_title": "Active Listings (Count)",
    },
    # 2) Median listing price
    "median_listing_price": {
        "file": county_base_dir / "housing_median_listing_price.csv",
        "value_col": "median_listing_price",
        "title": "Housing Inventory: Median Listing Price by County",
        "yaxis_title": "Median Listing Price (USD)",
    },
    # 3) New private housing units authorized
    "new_private_units": {
        "file": county_base_dir / "housing_new_private_units.csv",
        "value_col": "new_private_units",
        "title": "New Private Housing Units Authorized by County",
        "yaxis_title": "Units Authorized",
    },
    # 4) All-transactions house price index
    "house_price_index": {
        "file": county_base_dir / "housing_house_price_index.csv",
        "value_col": "house_price_index",
        "title": "All-Transactions House Price Index by County",
        "yaxis_title": "House Price Index (Index, 2017=100)",
    },
    # 5) Zillow home value index
    "zillow_hvi": {
        "file": county_base_dir / "housing_zillow_home_value_index.csv",
        "value_col": "zillow_home_value_index",
        "title": "Zillow Home Value Index (All Homes) by County",
        "yaxis_title": "Home Value Index (USD)",
    },
}

def make_housing_figure(metric_key: str):
    """
    Create a county-level housing Plotly line chart for the given metric_key.

    metric_key must be one of housing_metrics.keys().
    CSVs are expected to have at least: 'date', 'county_name', and value_col.
    """
    cfg = housing_metrics[metric_key]
    df = pd.read_csv(cfg["file"])

    # Adjust these column names if yours are slightly different
    # e.g. 'observation_date' instead of 'date'
    if "date" in df.columns:
        date_col = "date"
    elif "observation_date" in df.columns:
        date_col = "observation_date"
    else:
        raise ValueError("Could not find a date column in housing CSV")

    df[date_col] = pd.to_datetime(df[date_col])

    fig = px.line(
        df,
        x=date_col,
        y=cfg["value_col"],
        color="county_name",
        title=cfg["title"],
        labels={
            date_col: "Date",
            cfg["value_col"]: cfg["yaxis_title"],
            "county_name": "County",
        },
    )

    # Match style of your labor/econ figures
    fig.update_layout(
        hovermode="x unified",
        legend_title_text="County",
        margin=dict(l=40, r=10, t=80, b=40),
    )

    # Optional: tweak hover text to be clean
    fig.update_traces(
        hovertemplate="<b>%{customdata}</b><br>Date: %{x|%Y-%m-%d}<br>"
                      + f"{cfg['yaxis_title']}: %{y:,}<extra></extra>",
        customdata=df["county_name"],
    )

    return fig


# In[37]:


fig_active = make_housing_figure("active_listing_count")
fig_active.show()

fig_median_price = make_housing_figure("median_listing_price")
fig_median_price.show()

fig_units = make_housing_figure("new_private_units")
fig_units.show()

fig_hpi = make_housing_figure("house_price_index")
fig_hpi.show()

fig_zillow = make_housing_figure("zillow_hvi")
fig_zillow.show()


# In[38]:


from pathlib import Path

county_base_dir = Path("csv_outputs/county_data")
list(county_base_dir.glob("*.csv"))


# In[39]:


import glob

glob.glob("**/*housing*.csv", recursive=True)


# In[40]:


from pathlib import Path
import pandas as pd
import plotly.express as px

# Base dir – you already use this for labor/econ
county_base_dir = Path("csv_outputs/county_data")

# -------------------------------------------------
# HOUSING CONFIG
# -------------------------------------------------
housing_metric_configs = {
    # Housing inventory: active listing count
    "active_listing_count": {
        "file_suffix": "housing_inventory_active_listing_count.csv",
        "title": "Housing Inventory: Active Listing Count by County",
        "yaxis_title": "Active Listings (Count)",
    },
    # Housing inventory: median listing price (dollars)
    "median_listing_price": {
        "file_suffix": "housing_inventory_median_listing_price_dollars.csv",
        "title": "Housing Inventory: Median Listing Price by County",
        "yaxis_title": "Median Listing Price (USD)",
    },
    # New private housing units authorized by building permits
    "new_private_units": {
        "file_suffix": "new_private_housing_units_authorized_by_building_permits_count.csv",
        "title": "New Private Housing Units Authorized by County",
        "yaxis_title": "Units Authorized",
    },
}


def make_housing_figure(metric_key: str):
    """
    Build a county-level housing Plotly line chart.

    metric_key must be one of:
        'active_listing_count', 'median_listing_price', 'new_private_units'
    This function:
      * finds all county CSVs that match the metric's file_suffix
      * stacks them together
      * returns a Plotly figure
    """
    if metric_key not in housing_metric_configs:
        raise ValueError(f"metric_key must be one of {list(housing_metric_configs.keys())}")

    cfg = housing_metric_configs[metric_key]
    suffix = cfg["file_suffix"]

    # Find all county CSVs for this metric
    files = list(county_base_dir.rglob(f"*{suffix}"))
    if not files:
        raise FileNotFoundError(f"No files found under {county_base_dir} matching *{suffix}")

    frames = []

    for path in files:
        df = pd.read_csv(path)

        # Guess date + value columns robustly
        # 1) date column = first col whose name contains 'date'
        date_candidates = [c for c in df.columns if "date" in c.lower()]
        if date_candidates:
            date_col = date_candidates[0]
        else:
            date_col = df.columns[0]  # fallback: first column

        # 2) value column = last numeric column
        numeric_cols = df.select_dtypes(include="number").columns
        if len(numeric_cols) == 0:
            # fallback: assume second column is the value
            value_col = df.columns[1]
        else:
            value_col = numeric_cols[-1]

        # Parse county from folder name (e.g., 'prince_georges' -> 'Prince Georges')
        county_raw = path.parent.name
        county_name = county_raw.replace("_", " ").title()

        temp = pd.DataFrame({
            "date": pd.to_datetime(df[date_col]),
            "value": df[value_col],
            "county_name": county_name,
        })
        frames.append(temp)

    all_data = pd.concat(frames, ignore_index=True).sort_values("date")

    fig = px.line(
        all_data,
        x="date",
        y="value",
        color="county_name",
        title=cfg["title"],
        labels={
            "date": "Date",
            "value": cfg["yaxis_title"],
            "county_name": "County",
        },
    )

    fig.update_layout(
        hovermode="x unified",
        legend_title_text="County",
        margin=dict(l=40, r=10, t=80, b=40),
    )

    # Clean hover template: County, date, formatted value
    fig.update_traces(
        hovertemplate="<b>%{customdata}</b><br>"
                      "Date: %{x|%Y-%m-%d}<br>"
                      f"{cfg['yaxis_title']}: %{y:,.0f}<extra></extra>",
        customdata=all_data["county_name"],
    )

    return fig


# In[41]:


fig_active = make_housing_figure("active_listing_count")
fig_active.show()

fig_price = make_housing_figure("median_listing_price")
fig_price.show()

fig_units = make_housing_figure("new_private_units")
fig_units.show()


# In[ ]:




