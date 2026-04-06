"""
Maryland Counties FRED Data Aggregator (Automated)
==================================================

Pulls monthly & annual FRED/BLS metrics for all 24 Maryland counties
using `fredapi`. Generates:
1. One CSV per county (for Tableau linkage)
2. A master combined dataset
3. A data dictionary summarizing all series IDs
4. A text-based pipeline summary (no Graphviz dependency)

Author: Joshua Kwan
Date: 11/11/2025
"""

import os
import time
import pandas as pd
from fredapi import Fred
from functools import reduce
from tqdm import tqdm

# ======================================================
# CONFIGURATION
# ======================================================

# Load .env if available (useful for local testing)
#load_dotenv()

# Get FRED API key from environment variable (preferred)
API_KEY = os.getenv("FRED_API_KEY")

if not API_KEY:
    raise ValueError("❌ Missing FRED_API_KEY. Please set it as an environment variable or in GitHub Secrets.")

fred = Fred(api_key=API_KEY)
SLEEP_TIME = 0.25

COUNTY_EXPORT_PATH = "data/counties/"
MASTER_EXPORT_PATH = "data/master/"
os.makedirs(COUNTY_EXPORT_PATH, exist_ok=True)
os.makedirs(MASTER_EXPORT_PATH, exist_ok=True)

DATA_DICT_PATH = os.path.join("data", "data_dictionary.csv")
SUMMARY_PATH = os.path.join("data", "pipeline_summary.txt")

# ======================================================
# COUNTY SERIES DEFINITIONS
# ======================================================
# ⚠️ Paste your full verified COUNTIES = {...} dictionary here exactly as before.

COUNTIES = {
    # Code -> name + series IDs
    # Employment Count is LAUS "Employment Level": LAUCN + state(24) + county(FIPS3) + area(0000000) + 05
    # Example: Allegany (FIPS 001) -> LAUCN240010000000005
    "AG": {
        "County": "Allegany",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24001A", "A"),
            "Civilian_Labor_Force": ("MDALLE0LFN", "M"),
            "Employment_Count": ("LAUCN240010000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24001A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24001", "M"),
            "Median_Listing_Price": ("MEDLISPRI24001", "M"),
            "Permits_Annual": ("BPPRIV024001", "A"),
            "Real_GDP": ("REALGDPALL24001", "A"),
            "Population_Annual": ("MDALLE0POP", "A"),
            "Unemployed_Persons": ("LAUCN240010000000004", "M"),
            "Unemployment_Rate": ("MDALLE0URN", "M"),
        },
    },
    "AA": {
        "County": "Anne Arundel",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24003A", "A"),
            "Civilian_Labor_Force": ("MDANNE5LFN", "M"),
            "Employment_Count": ("LAUCN240030000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24003A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24003", "M"),
            "Median_Listing_Price": ("MEDLISPRI24003", "M"),
            "Permits_Annual": ("BPPRIV024003", "A"),
            "Real_GDP": ("REALGDPALL24003", "A"),
            "Population_Annual": ("MDANNE5POP", "A"),
            "Unemployed_Persons": ("LAUCN240030000000004", "M"),
            "Unemployment_Rate": ("MDANNE5URN", "M"),
        },
    },
    "BALT": {
        "County": "Baltimore County",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24005A", "A"),
            "Civilian_Labor_Force": ("MDBALT0LFN", "M"),
            "Employment_Count": ("LAUCN240050000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24005A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24005", "M"),
            "Median_Listing_Price": ("MEDLISPRI24005", "M"),
            "Permits_Annual": ("BPPRIV024005", "A"),
            "Real_GDP": ("REALGDPALL24005", "A"),
            "Population_Annual": ("MDBALT0POP", "A"),
            "Unemployed_Persons": ("LAUCN240050000000004", "M"),
            "Unemployment_Rate": ("MDBALT0URN", "M"),
        },
    },
    "BALT_CITY": {
        "County": "Baltimore City",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24510A", "A"),
            "Civilian_Labor_Force": ("MDBALT5LFN", "M"),
            # client alt option was CES "SMS24925810000000001"
            "Employment_Count": ("SMS24925810000000001", "M"),
            "Poverty_All_Ages": ("PPAAMD24510A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24510", "M"),
            "Median_Listing_Price": ("MEDLISPRI24510", "M"),
            "Permits_Annual": ("BPPRIV024510", "A"),
            "Real_GDP": ("REALGDPALL24510", "A"),
            "Population_Annual": ("MDBALT5POP", "A"),
            "Unemployed_Persons": ("LAUCN245100000000004", "M"),
            "Unemployment_Rate": ("MDBALT5URN", "M"),
        },
    },
    "CAL": {
        "County": "Calvert",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24009A", "A"),
            "Civilian_Labor_Force": ("MDCALV9LFN", "M"),
            "Employment_Count": ("LAUCN240090000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24009A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24009", "M"),
            "Median_Listing_Price": ("MEDLISPRI24009", "M"),
            "Permits_Annual": ("BPPRIV024009", "A"),
            "Real_GDP": ("REALGDPALL24009", "A"),
            "Population_Annual": ("MDCALV9POP", "A"),
            "Unemployed_Persons": ("LAUCN240090000000004", "M"),
            "Unemployment_Rate": ("MDCALV9URN", "M"),
        },
    },
    "CAR": {
        "County": "Caroline",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24011A", "A"),
            "Civilian_Labor_Force": ("MDCARO1LFN", "M"),
            "Employment_Count": ("LAUCN240110000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24011A156NCEN", "A"),
            # No active listings / median listing price for Caroline
            "Permits_Annual": ("BPPRIV024011", "A"),
            "Real_GDP": ("REALGDPALL24011", "A"),
            "Population_Annual": ("MDCARO1POP", "A"),
            "Unemployed_Persons": ("LAUCN240110000000004", "M"),
            "Unemployment_Rate": ("MDCARO1URN", "M"),
        },
    },
    "CARR": {
        "County": "Carroll",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24013A", "A"),
            "Civilian_Labor_Force": ("MDCARR5LFN", "M"),
            "Employment_Count": ("LAUCN240130000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24013A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24013", "M"),
            "Median_Listing_Price": ("MEDLISPRI24013", "M"),
            "Permits_Annual": ("BPPRIV024013", "A"),
            "Real_GDP": ("REALGDPALL24013", "A"),
            "Population_Annual": ("MDCARR5POP", "A"),
            "Unemployed_Persons": ("LAUCN240130000000004", "M"),
            "Unemployment_Rate": ("MDCARR5URN", "M"),
        },
    },
    "CEC": {
        "County": "Cecil",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24015A", "A"),
            "Civilian_Labor_Force": ("MDCECI0LFN", "M"),
            "Employment_Count": ("LAUCN240150000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24015A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24015", "M"),
            "Median_Listing_Price": ("MEDLISPRI24015", "M"),
            "Permits_Annual": ("BPPRIV024015", "A"),
            "Real_GDP": ("REALGDPALL24015", "A"),
            "Population_Annual": ("MDCECI0POP", "A"),
            "Unemployed_Persons": ("LAUCN240150000000004", "M"),
            "Unemployment_Rate": ("MDCECI0URN", "M"),
        },
    },
    "CHA": {
        "County": "Charles",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24017A", "A"),
            "Civilian_Labor_Force": ("MDCHAR0LFN", "M"),
            "Employment_Count": ("LAUCN240170000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24017A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24017", "M"),
            "Median_Listing_Price": ("MEDLISPRI24017", "M"),
            "Permits_Annual": ("BPPRIV024017", "A"),
            "Real_GDP": ("REALGDPALL24017", "A"),
            "Population_Annual": ("MDCHAR0POP", "A"),
            "Unemployed_Persons": ("LAUCN240170000000004", "M"),
            "Unemployment_Rate": ("MDCHAR0URN", "M"),
        },
    },
    "DOR": {
        "County": "Dorchester",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24019A", "A"),
            "Civilian_Labor_Force": ("MDDORC9LFN", "M"),
            "Employment_Count": ("LAUCN240190000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24019A156NCEN", "A"),
            # No active listings / median listing price for Dorchester
            "Permits_Annual": ("BPPRIV024019", "A"),
            "Real_GDP": ("REALGDPALL24019", "A"),
            "Population_Annual": ("MDDORC9POP", "A"),
            "Unemployed_Persons": ("LAUCN240190000000004", "M"),
            "Unemployment_Rate": ("MDDORC9URN", "M"),
        },
    },
    "FRE": {
        "County": "Frederick",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24021A", "A"),
            "Civilian_Labor_Force": ("MDFRED5LFN", "M"),
            "Employment_Count": ("LAUCN240210000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24021A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24021", "M"),
            "Median_Listing_Price": ("MEDLISPRI24021", "M"),
            "Permits_Annual": ("BPPRIV024021", "A"),
            "Real_GDP": ("REALGDPALL24021", "A"),
            "Population_Annual": ("MDFRED5POP", "A"),
            "Unemployed_Persons": ("LAUCN240210000000004", "M"),
            "Unemployment_Rate": ("MDFRED5URN", "M"),
        },
    },
    "GAR": {
        "County": "Garrett",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24023A", "A"),
            "Civilian_Labor_Force": ("MDGARR3LFN", "M"),
            "Employment_Count": ("LAUCN240230000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24023A156NCEN", "A"),
            # No active listings / median listing price for Garrett
            "Permits_Annual": ("BPPRIV024023", "A"),
            "Real_GDP": ("REALGDPALL24023", "A"),
            "Population_Annual": ("MDGARR3POP", "A"),
            "Unemployed_Persons": ("LAUCN240230000000004", "M"),
            "Unemployment_Rate": ("MDGARR3URN", "M"),
        },
    },
    "HAR": {
        "County": "Harford",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24025A", "A"),
            "Civilian_Labor_Force": ("MDHARF0LFN", "M"),
            "Employment_Count": ("LAUCN240250000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24025A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24025", "M"),
            "Median_Listing_Price": ("MEDLISPRI24025", "M"),
            "Permits_Annual": ("BPPRIV024025", "A"),
            "Real_GDP": ("REALGDPALL24025", "A"),
            "Population_Annual": ("MDHARF0POP", "A"),
            "Unemployed_Persons": ("LAUCN240250000000004", "M"),
            "Unemployment_Rate": ("MDHARF0URN", "M"),
        },
    },
    "HOW": {
        "County": "Howard",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24027A", "A"),
            "Civilian_Labor_Force": ("MDHOWA0LFN", "M"),
            "Employment_Count": ("LAUCN240270000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24027A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24027", "M"),
            "Median_Listing_Price": ("MEDLISPRI24027", "M"),
            "Permits_Annual": ("BPPRIV024027", "A"),
            "Real_GDP": ("REALGDPALL24027", "A"),
            "Population_Annual": ("MDHOWA0POP", "A"),
            "Unemployed_Persons": ("LAUCN240270000000004", "M"),
            "Unemployment_Rate": ("MDHOWA0URN", "M"),
        },
    },
    "KENT": {
        "County": "Kent",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24029A", "A"),
            "Civilian_Labor_Force": ("MDKENT9LFN", "M"),
            "Employment_Count": ("LAUCN240290000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24029A156NCEN", "A"),
            # No active listings / median listing price for Kent
            "Permits_Annual": ("BPPRIV024029", "A"),
            "Real_GDP": ("REALGDPALL24029", "A"),
            "Population_Annual": ("MDKENT9POP", "A"),
            "Unemployed_Persons": ("LAUCN240290000000004", "M"),
            "Unemployment_Rate": ("MDKENT9URN", "M"),
        },
    },
    "MON": {
        "County": "Montgomery",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24031A", "A"),
            "Civilian_Labor_Force": ("MDMONT0LFN", "M"),
            "Employment_Count": ("LAUCN240310000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24031A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24031", "M"),
            "Median_Listing_Price": ("MEDLISPRI24031", "M"),
            "Permits_Annual": ("BPPRIV024031", "A"),
            "Real_GDP": ("REALGDPALL24031", "A"),
            "Population_Annual": ("MDMONT0POP", "A"),
            "Unemployed_Persons": ("LAUCN240310000000004", "M"),
            "Unemployment_Rate": ("MDMONT0URN", "M"),
        },
    },
    "PG": {
        "County": "Prince George's",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24033A", "A"),
            "Civilian_Labor_Force": ("MDPRIN5LFN", "M"),
            "Employment_Count": ("LAUCN240330000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24033A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24033", "M"),
            "Median_Listing_Price": ("MEDLISPRI24033", "M"),
            "Permits_Annual": ("BPPRIV024033", "A"),
            "Real_GDP": ("REALGDPALL24033", "A"),
            "Population_Annual": ("MDPRIN5POP", "A"),
            "Unemployed_Persons": ("LAUCN240330000000004", "M"),
            "Unemployment_Rate": ("MDPRIN5URN", "M"),
        },
    },
    "QA": {
        "County": "Queen Anne's",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24035A", "A"),
            "Civilian_Labor_Force": ("MDQUEE5LFN", "M"),
            "Employment_Count": ("LAUCN240350000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24035A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24035", "M"),
            "Median_Listing_Price": ("MEDLISPRI24035", "M"),
            "Permits_Annual": ("BPPRIV024035", "A"),
            "Real_GDP": ("REALGDPALL24035", "A"),
            "Population_Annual": ("MDQUEE5POP", "A"),
            "Unemployed_Persons": ("LAUCN240350000000004", "M"),
            "Unemployment_Rate": ("MDQUEE5URN", "M"),
        },
    },
    "SOM": {
        "County": "Somerset",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24039A", "A"),
            "Civilian_Labor_Force": ("MDSOME9LFN", "M"),
            "Employment_Count": ("LAUCN240390000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24039A156NCEN", "A"),
            # No active listings / median listing price for Somerset
            "Permits_Annual": ("BPPRIV024039", "A"),
            "Real_GDP": ("REALGDPALL24039", "A"),
            "Population_Annual": ("MDSOME9POP", "A"),
            "Unemployed_Persons": ("LAUCN240390000000004", "M"),
            "Unemployment_Rate": ("MDSOME9URN", "M"),
        },
    },
    "STM": {
        "County": "St. Mary's",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24037A", "A"),
            "Civilian_Labor_Force": ("MDSTMA5LFN", "M"),
            "Employment_Count": ("LAUCN240370000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24037A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24037", "M"),
            "Median_Listing_Price": ("MEDLISPRI24037", "M"),
            "Permits_Annual": ("BPPRIV024037", "A"),
            "Real_GDP": ("REALGDPALL24037", "A"),
            "Population_Annual": ("MDSTMA5POP", "A"),
            "Unemployed_Persons": ("LAUCN240370000000004", "M"),
            "Unemployment_Rate": ("MDSTMA5URN", "M"),
        },
    },
    "TAL": {
        "County": "Talbot",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24041A", "A"),
            "Civilian_Labor_Force": ("MDTALB1LFN", "M"),
            "Employment_Count": ("LAUCN240410000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24041A156NCEN", "A"),
            # No active listings / median listing price for Talbot
            "Permits_Annual": ("BPPRIV024041", "A"),
            "Real_GDP": ("REALGDPALL24041", "A"),
            "Population_Annual": ("MDTALB1POP", "A"),
            "Unemployed_Persons": ("LAUCN240410000000004", "M"),
            "Unemployment_Rate": ("MDTALB1URN", "M"),
        },
    },
    "WAS": {
        "County": "Washington",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24043A", "A"),
            "Civilian_Labor_Force": ("MDWASH5LFN", "M"),
            "Employment_Count": ("LAUCN240430000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24043A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24043", "M"),
            "Median_Listing_Price": ("MEDLISPRI24043", "M"),
            "Permits_Annual": ("BPPRIV024043", "A"),
            "Real_GDP": ("REALGDPALL24043", "A"),
            "Population_Annual": ("MDWASH5POP", "A"),
            "Unemployed_Persons": ("LAUCN240430000000004", "M"),
            "Unemployment_Rate": ("MDWASH5URN", "M"),
        },
    },
    "WIC": {
        "County": "Wicomico",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24045A", "A"),
            "Civilian_Labor_Force": ("MDWICO5LFN", "M"),
            "Employment_Count": ("LAUCN240450000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24045A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24045", "M"),  # confirmed Active Listings
            "Median_Listing_Price": ("MEDLISPRI24045", "M"),
            "Permits_Annual": ("BPPRIV024045", "A"),
            "Real_GDP": ("REALGDPALL24045", "A"),
            "Population_Annual": ("MDWICO5POP", "A"),
            "Unemployed_Persons": ("LAUCN240450000000004", "M"),
            "Unemployment_Rate": ("MDWICO5URN", "M"),
        },
    },
    "WOR": {
        "County": "Worcester",
        "series": {
            "HPI_AllTransactions": ("ATNHPIUS24047A", "A"),
            "Civilian_Labor_Force": ("MDWORC7LFN", "M"),
            "Employment_Count": ("LAUCN240470000000005", "M"),
            "Poverty_All_Ages": ("PPAAMD24047A156NCEN", "A"),
            "Active_Listings": ("ACTLISCOU24047", "M"),
            "Median_Listing_Price": ("MEDLISPRI24047", "M"),
            "Permits_Annual": ("BPPRIV024047", "A"),
            "Real_GDP": ("REALGDPALL24047", "A"),
            "Population_Annual": ("MDWORC7POP", "A"),
            "Unemployed_Persons": ("LAUCN240470000000004", "M"),
            "Unemployment_Rate": ("MDWORC7URN", "M"),
        },
    },
}

# from masterdatasetmulticounty_counties import COUNTIES  # optional import if separated


# ======================================================
# HELPER FUNCTIONS
# ======================================================

def period_to_month_end(series: pd.Series, freq: str) -> pd.DataFrame:
    """Convert a FRED series to a monthly/annual dataframe with month-end timestamps."""
    df = pd.DataFrame({"Date": pd.to_datetime(series.index, errors="coerce"), "Value": series.values})
    df = df.dropna(subset=["Date"])

    if freq == "M":
        df["Date"] = df["Date"].dt.to_period("M").dt.to_timestamp("M")
    else:
        df["Date"] = df["Date"].dt.to_period("Y").dt.to_timestamp("M")
        df = df.set_index("Date").resample("ME").ffill().reset_index()

    return df


def build_county_df(code: str, county_data: dict) -> pd.DataFrame:
    """Fetch and merge all available series for a single county."""
    frames = []

    for col_name, (series_id, freq) in tqdm(county_data["series"].items(), desc=f"Loading {code}", leave=False):
        try:
            time.sleep(SLEEP_TIME)
            series = fred.get_series(series_id)
            if series is None or series.empty:
                print(f"⚠️ {code} {col_name}: empty or invalid series {series_id}")
                continue

            df = period_to_month_end(series, freq).rename(columns={"Value": col_name})
            frames.append(df)
        except Exception as err:
            print(f"⚠️ {code} {col_name}: failed to load {series_id} -> {err}")

    if not frames:
        return pd.DataFrame()

    merged = reduce(lambda l, r: pd.merge(l, r, on="Date", how="outer"), frames)
    merged.insert(1, "County", county_data["County"])
    merged.insert(2, "County_Code", code)

    # Order columns
    metric_cols = sorted([c for c in merged.columns if c not in ["Date", "County", "County_Code"]])
    merged = merged[["Date", "County", "County_Code"] + metric_cols]

    return merged.sort_values("Date").reset_index(drop=True)


def generate_data_dictionary():
    """Generate a CSV dictionary mapping each county & metric to its FRED series ID."""
    rows = []
    for code, meta in COUNTIES.items():
        for metric, (sid, freq) in meta["series"].items():
            rows.append({
                "County_Code": code,
                "County_Name": meta["County"],
                "Metric": metric,
                "Series_ID": sid,
                "Frequency": "Monthly" if freq == "M" else "Annual"
            })
    df = pd.DataFrame(rows)
    df.to_csv(DATA_DICT_PATH, index=False)
    print(f"🗂️ Data dictionary saved to {DATA_DICT_PATH}")


def generate_pipeline_summary():
    """Write a simple text-based summary of the ETL pipeline."""
    summary = """
Maryland FRED Data Pipeline Summary
===================================

Data Source:
    → FRED API (24 Maryland counties, various BLS & Census indicators)

ETL Steps:
    1. Pull raw FRED time-series data for each indicator.
    2. Convert annual/monthly data to month-end timestamps.
    3. Merge county-level metrics into unified DataFrames.
    4. Save per-county CSVs under /data/counties.
    5. Combine all counties into master dataset under /data/master.
    6. Generate data_dictionary.csv documenting series and frequencies.

Output Files:
    - data/counties/<County>.csv
    - data/master/maryland_master.csv
    - data/data_dictionary.csv
    - data/pipeline_summary.txt

Integration:
    - Tableau connects directly to /data/master/maryland_master.csv
    - Optional: Automate monthly GitHub Action refresh
"""
    with open(SUMMARY_PATH, "w") as f:
        f.write(summary)
    print(f"📝 Pipeline summary written to {SUMMARY_PATH}")


# ======================================================
# MAIN EXECUTION
# ======================================================

def main():
    start_time = time.time()
    print("📊 Fetching FRED/BLS data for all Maryland counties...\n")
    all_dfs = []

    for code, meta in COUNTIES.items():
        df = build_county_df(code, meta)

        if df.empty:
            print(f"❗ Skipped {meta['County']} ({code}) - no data found.")
            continue

        file_name = f"{meta['County'].replace(' ', '_')}.csv"
        csv_path = os.path.join(COUNTY_EXPORT_PATH, file_name)
        df.to_csv(csv_path, index=False)
        print(f"✅ Exported: {csv_path}")
        all_dfs.append(df)

    # Merge into master dataset
    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True).sort_values(["County", "Date"])
        master_path = os.path.join(MASTER_EXPORT_PATH, "maryland_master.csv")
        master_df.to_csv(master_path, index=False)
        print(f"\n🎉 Master dataset saved: {master_path}")
        print(f"📊 {master_df.shape[0]} rows × {master_df.shape[1]} columns")
    else:
        print("\n❗ No valid data retrieved. Check FRED connection or series IDs.")

    # Documentation utilities
    generate_data_dictionary()
    generate_pipeline_summary()

    print(f"\n⏱️ Runtime: {time.time() - start_time:.2f} seconds")

# ----------------------------------------------
# OPTIONAL: Export master dataset as .hyper file
# ----------------------------------------------
try:
    from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter

    hyper_path = os.path.join(MASTER_EXPORT_PATH, "maryland_master.hyper")
    print(f"\n⚙️ Creating Tableau .hyper extract at: {hyper_path}")

    # Start Hyper process
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=True) as connection:

            # Build table schema dynamically
            cols = [
                TableDefinition.Column(name, SqlType.text()) 
                for name in master_df.columns
            ]
            table = TableDefinition("Extract", "Extract", cols)

            connection.catalog.create_schema("Extract")
            connection.catalog.create_table(table)

            # Insert rows
            with Inserter(connection, table) as inserter:
                for _, row in master_df.iterrows():
                    inserter.add_row(list(row.values))
                inserter.execute()

    print(f"✅ Tableau .hyper file generated: {hyper_path}")

except ImportError:
    print("ℹ️ tableauhyperapi not installed — skipping .hyper export.")

# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    main()

print("\n✅ FRED pipeline completed successfully and ready for Tableau integration!")
