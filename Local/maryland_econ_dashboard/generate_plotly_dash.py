# =============================================================================
# Maryland Economic Dashboard
# generate_plotly_dash.py
#
# Rewritten to fetch BLS and FRED data live via API (no CSV files needed).
# Uses flask-caching so data is refreshed monthly without any manual runs.
# Compatible with Render deployment and local development.
#
# Local usage:
#   1. Create a .env file in the repo root (see README for format)
#   2. pip install -r requirements.txt
#   3. python generate_plotly_dash.py
#   4. Open http://127.0.0.1:8050 in your browser
#
# Render usage:
#   - Set BLS_API_KEY and FRED_API_KEY as environment variables in Render dashboard
#   - Start command: gunicorn generate_plotly_dash:server
# =============================================================================

# ---------------------------------------- #
# Imports                                  #
# ---------------------------------------- #

import os
import re
import json
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

from fredapi import Fred
from dash import Dash, html, dcc, Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from flask_caching import Cache

# Load .env file when running locally.
# On Render, environment variables are set directly in the dashboard
# so python-dotenv is only used locally and gracefully skipped if not installed.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------- #
# API Keys (from environment variables)    #
# ---------------------------------------- #

BLS_API_KEY  = os.getenv("BLS_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

if not BLS_API_KEY:
    raise EnvironmentError(
        "BLS_API_KEY not set. Add it to your .env file locally, "
        "or set it as an environment variable in Render."
    )
if not FRED_API_KEY:
    raise EnvironmentError(
        "FRED_API_KEY not set. Add it to your .env file locally, "
        "or set it as an environment variable in Render."
    )

fred_client = Fred(api_key=FRED_API_KEY)

# ---------------------------------------- #
# App Initialisation                       #
# ---------------------------------------- #

app = Dash(__name__)

# Expose the Flask server so gunicorn can find it (required for Render)
server = app.server

# Allow the app to be embedded in an iframe on the NCSG WordPress site
@server.after_request
def apply_iframe_headers(response):
    response.headers["X-Frame-Options"] = "ALLOW-FROM https://www.umdsmartgrowth.org"
    response.headers["Content-Security-Policy"] = (
        "frame-ancestors 'self' https://www.umdsmartgrowth.org"
    )
    return response

# ---------------------------------------- #
# Cache Setup                              #
# ---------------------------------------- #
# Data is cached in memory for 30 days.
# After 30 days the next request will trigger a fresh API call automatically.
# No manual runs, no CSV files, no redeployments needed for data updates.

THIRTY_DAYS = 60 * 60 * 24 * 30

cache = Cache(server, config={
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": THIRTY_DAYS,
})

# ---------------------------------------- #
# Helper Functions                         #
# ---------------------------------------- #

def to_snake_case(s: str) -> str:
    """Convert a string to snake_case suitable for filenames and comparisons."""
    s = s.lower()
    s = re.sub(r"[ /\\\-]", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")

def make_friendly_label(metric_name: str) -> str:
    """Convert a snake_case metric name to a readable title."""
    return metric_name.replace("_", " ").title()

def get_year_range(lookback_years: int = 10):
    """Return a (start_year, end_year) tuple covering the last N years."""
    current = datetime.now().year
    return str(current - lookback_years), str(current)

# ---------------------------------------- #
# Load Series IDs from Excel               #
# ---------------------------------------- #
# The Excel file is committed to the repo so it's available on Render too.

EXCEL_PATH = Path(__file__).parent / "Indicators Series ID List.xlsx"

def load_county_bls_ids() -> pd.DataFrame:
    """
    Load the COUNTY BLS sheet and derive series IDs for all four metrics.
    BLS suffix codes: 05=Employment, 04=Unemployment Count,
                      03=Unemployment Rate, 06=Labor Force
    Returns a DataFrame with columns: COUNTY, Metric, Series ID
    """
    df = pd.read_excel(EXCEL_PATH, sheet_name="COUNTY BLS", skiprows=0)
    df.columns = ["COUNTY", "SERIES ID"]
    df["COUNTY"] = df["COUNTY"].str.strip()

    # Fix Baltimore City (missing from sheet)
    df.loc[df["COUNTY"] == "Baltimore City", "SERIES ID"] = "LAUCN245100000000005"

    # Derive the other three metrics from the employment base ID
    df["Unemployment Count ID"] = df["SERIES ID"].str.replace("05$", "04", regex=True)
    df["Unemployment Rate ID"]  = df["SERIES ID"].str.replace("05$", "03", regex=True)
    df["Labor Force ID"]        = df["SERIES ID"].str.replace("05$", "06", regex=True)

    # Melt so every row is one (county, metric, series_id) triplet
    melted = df.melt(
        id_vars=["COUNTY"],
        value_vars=["SERIES ID", "Unemployment Count ID",
                    "Unemployment Rate ID", "Labor Force ID"],
        var_name="Metric",
        value_name="Series ID",
    )
    melted["Metric"] = melted["Metric"].map({
        "SERIES ID":            "Employment",
        "Unemployment Count ID": "Unemployment Count",
        "Unemployment Rate ID":  "Unemployment Rate",
        "Labor Force ID":        "Labor Force",
    })
    return melted


def load_county_fred_ids() -> pd.DataFrame:
    """
    Load the COUNTY FRED sheet.
    Returns a DataFrame with columns: COUNTY + one column per metric series.
    """
    df = pd.read_excel(EXCEL_PATH, sheet_name="COUNTY FRED", skiprows=1)
    df["COUNTY"] = df["COUNTY"].astype(str).str.strip()
    return df


def load_state_fred_ids() -> pd.DataFrame:
    """
    Load the MD FRED sheet.
    Returns a DataFrame with columns: DATA TYPE, SERIES ID
    """
    return pd.read_excel(EXCEL_PATH, sheet_name="MD FRED")


# Load once at startup — these are just metadata, not the actual data
COUNTY_BLS_IDS   = load_county_bls_ids()
COUNTY_FRED_IDS  = load_county_fred_ids()
STATE_FRED_IDS   = load_state_fred_ids()

# Sorted pretty-name county list for dropdowns
COUNTY_LIST = sorted(COUNTY_BLS_IDS["COUNTY"].unique().tolist())

# ---------------------------------------- #
# BLS Data Fetching (cached 30 days)       #
# ---------------------------------------- #

MONTH_LOOKUP = {
    "M01": "01", "M02": "02", "M03": "03", "M04": "04",
    "M05": "05", "M06": "06", "M07": "07", "M08": "08",
    "M09": "09", "M10": "10", "M11": "11", "M12": "12",
}

SLEEP_TIME  = 0.5   # seconds between batches (be nice to the API)
CHUNK_SIZE  = 25    # BLS API limit per request
MAX_RETRIES = 3


def _fetch_bls_batch(series_ids: list, start_year: str, end_year: str) -> list:
    """
    Make one BLS API call for up to 25 series.
    Returns the raw list of series results, or [] on failure.
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                headers={"Content-type": "application/json"},
                data=json.dumps({
                    "seriesid":       series_ids,
                    "startyear":      start_year,
                    "endyear":        end_year,
                    "registrationkey": BLS_API_KEY,
                }),
                timeout=30,
            )
            payload = response.json()
            if payload.get("status") == "REQUEST_NOT_PROCESSED":
                print(f"[WARN] BLS batch rejected: {payload.get('message')}")
                return []
            return payload.get("Results", {}).get("series", [])
        except Exception as e:
            wait = 5 * (attempt + 1)
            print(f"[WARN] BLS request failed ({e}). Retrying in {wait}s...")
            time.sleep(wait)
    return []


@cache.memoize(timeout=THIRTY_DAYS)
def get_bls_data() -> pd.DataFrame:
    """
    Fetch all BLS labor metrics for all Maryland counties.
    Cached for 30 days — the first call after cache expiry re-fetches live data.

    Returns a DataFrame with columns:
        county (str), metric (str), date (datetime), value (float)
    """
    print("[INFO] Fetching fresh BLS data from API...")
    start_year, end_year = get_year_range(lookback_years=10)

    # Build a lookup: series_id -> (county_snake, metric_label)
    id_to_info = {
        row["Series ID"]: (to_snake_case(row["COUNTY"]), row["Metric"])
        for _, row in COUNTY_BLS_IDS.iterrows()
    }

    all_series_ids = list(id_to_info.keys())
    rows = []

    for i in range(0, len(all_series_ids), CHUNK_SIZE):
        chunk = all_series_ids[i : i + CHUNK_SIZE]
        print(f"[BLS] Fetching batch {i // CHUNK_SIZE + 1} ({len(chunk)} series)...")

        series_results = _fetch_bls_batch(chunk, start_year, end_year)

        for series in series_results:
            series_id = series["seriesID"]
            county_snake, metric = id_to_info.get(series_id, (None, None))
            if not county_snake:
                continue
            for item in series.get("data", []):
                period = item["period"]
                if not period.startswith("M"):
                    continue
                month = MONTH_LOOKUP.get(period)
                if not month:
                    continue
                try:
                    rows.append({
                        "county": county_snake,
                        "metric": metric,
                        "date":   pd.Timestamp(f"{item['year']}-{month}-01"),
                        "value":  float(item["value"]),
                    })
                except (ValueError, KeyError):
                    continue

        time.sleep(SLEEP_TIME)

    df = pd.DataFrame(rows)
    print(f"[INFO] BLS fetch complete. {len(df)} rows loaded.")
    return df


# ---------------------------------------- #
# FRED Data Fetching (cached 30 days)      #
# ---------------------------------------- #

def _safe_get_fred_series(series_id: str) -> pd.Series | None:
    """Fetch a single FRED series with retry on rate limit."""
    for attempt in range(MAX_RETRIES):
        try:
            return fred_client.get_series(series_id)
        except Exception as e:
            if "Too Many Requests" in str(e):
                wait = 5 * (attempt + 1)
                print(f"[WARN] FRED rate limit for {series_id}. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"[WARN] Could not fetch FRED series {series_id}: {e}")
                return None
    return None


@cache.memoize(timeout=THIRTY_DAYS)
def get_county_fred_data() -> pd.DataFrame:
    """
    Fetch all FRED county-level series for Maryland.
    Cached for 30 days.

    Returns a DataFrame with columns:
        county (str), metric (str), date (datetime), value (float)
    """
    print("[INFO] Fetching fresh county FRED data from API...")
    rows = []

    for _, row in COUNTY_FRED_IDS.iterrows():
        county = to_snake_case(str(row["COUNTY"]))
        for col in COUNTY_FRED_IDS.columns[1:]:
            series_id = row[col]
            if pd.isna(series_id) or str(series_id).strip() == "":
                continue
            data = _safe_get_fred_series(str(series_id).strip())
            if data is None or data.empty:
                continue
            metric = to_snake_case(str(col))
            for date, value in data.items():
                if pd.isna(value):
                    continue
                rows.append({
                    "county": county,
                    "metric": metric,
                    "date":   pd.Timestamp(date),
                    "value":  float(value),
                })
            time.sleep(0.25)

    df = pd.DataFrame(rows)
    print(f"[INFO] County FRED fetch complete. {len(df)} rows loaded.")
    return df


@cache.memoize(timeout=THIRTY_DAYS)
def get_state_fred_data() -> pd.DataFrame:
    """
    Fetch all FRED state-level series for Maryland.
    Cached for 30 days.

    Returns a DataFrame with columns:
        metric (str), date (datetime), value (float)
    """
    print("[INFO] Fetching fresh state FRED data from API...")
    rows = []

    for _, row in STATE_FRED_IDS.iterrows():
        series_id  = str(row.get("SERIES ID", "")).strip()
        metric_name = to_snake_case(str(row.get("DATA TYPE", "")).strip())
        if not series_id or not metric_name:
            continue
        data = _safe_get_fred_series(series_id)
        if data is None or data.empty:
            continue
        for date, value in data.items():
            if pd.isna(value):
                continue
            rows.append({
                "metric": metric_name,
                "date":   pd.Timestamp(date),
                "value":  float(value),
            })
        time.sleep(0.25)

    df = pd.DataFrame(rows)
    print(f"[INFO] State FRED fetch complete. {len(df)} rows loaded.")
    return df


# ---------------------------------------- #
# Metric Group Classification              #
# ---------------------------------------- #

GROUP_PATTERNS = {
    "housing": re.compile(r"house|housing|zillow|listing|building|permits", re.IGNORECASE),
    "labor":   re.compile(r"employ|labor|labour|unemployment|labor_force", re.IGNORECASE),
    "economy": re.compile(r"poverty|gdp|population|income|business|earnings|wage", re.IGNORECASE),
}

def classify_metric(metric_name: str) -> str | None:
    """Return the group name for a metric, or None if unclassified."""
    for group, pattern in GROUP_PATTERNS.items():
        if pattern.search(metric_name):
            return group
    return None


# ---------------------------------------- #
# Data Access Helpers for Callbacks        #
# ---------------------------------------- #

def get_labor_data_for_county(county_pretty: str) -> pd.DataFrame:
    """Return BLS labor data for one county."""
    county_snake = to_snake_case(county_pretty)
    df = get_bls_data()
    return df[df["county"] == county_snake].copy()


def get_fred_data_for_county(county_pretty: str, group: str) -> pd.DataFrame:
    """Return FRED data for one county filtered to a metric group."""
    county_snake = to_snake_case(county_pretty)
    df = get_county_fred_data()
    df = df[df["county"] == county_snake].copy()
    df["group"] = df["metric"].apply(classify_metric)
    return df[df["group"] == group].copy()


# ---------------------------------------- #
# Figure Builder                           #
# ---------------------------------------- #

def build_figure(df: pd.DataFrame, county_name: str, group_name: str) -> go.Figure:
    """
    Build a stacked subplot figure — one panel per metric.
    df must have columns: date, value, metric
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5, xref="paper", yref="paper",
            text=f"No {group_name.title()} data available for {county_name}.",
            showarrow=False, font=dict(size=16),
        )
        fig.update_layout(
            xaxis=dict(visible=False), yaxis=dict(visible=False),
            title=f"{county_name} – {group_name.title()} Metrics",
            height=300,
        )
        return fig

    metrics = sorted(df["metric"].unique())
    n_rows = len(metrics)

    fig = make_subplots(
        rows=n_rows, cols=1,
        shared_xaxes=True,
        subplot_titles=[make_friendly_label(m) for m in metrics],
        vertical_spacing=0.06,
    )

    for i, metric in enumerate(metrics, start=1):
        df_m = df[df["metric"] == metric].sort_values("date")
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["value"],
                mode="lines+markers",
                name=make_friendly_label(metric),
                showlegend=False,
                marker=dict(size=5),
                hovertemplate=(
                    f"<b>{make_friendly_label(metric)}</b><br>"
                    "Date: %{x|%Y-%m-%d}<br>"
                    "Value: %{y:,.2f}<extra></extra>"
                ),
            ),
            row=i, col=1,
        )
        fig.update_yaxes(title_text="Value", row=i, col=1)

    fig.update_xaxes(title_text="Date", row=n_rows, col=1)
    fig.update_layout(
        height=260 * n_rows,
        title_text=f"{county_name} – {group_name.title()} Metrics Over Time",
        margin=dict(l=50, r=20, t=80, b=40),
    )
    return fig


# ---------------------------------------- #
# App Layout                               #
# ---------------------------------------- #

GROUP_OPTIONS = ["labor", "housing", "economy"]

app.layout = html.Div(
    style={"fontFamily": "Arial, sans-serif", "maxWidth": "1100px", "margin": "0 auto", "padding": "20px"},
    children=[
        html.H2("Maryland County Economic Dashboard", style={"marginBottom": "4px"}),
        html.P(
            "Data sourced from BLS and FRED. Refreshed automatically every 30 days.",
            style={"color": "#666", "marginTop": "0", "marginBottom": "24px"},
        ),

        # Dropdowns row
        html.Div(
            style={"display": "flex", "gap": "24px", "marginBottom": "24px"},
            children=[
                html.Div([
                    html.Label("County", style={"fontWeight": "bold", "display": "block", "marginBottom": "6px"}),
                    dcc.Dropdown(
                        id="county_dropdown",
                        options=[{"label": c, "value": c} for c in COUNTY_LIST],
                        value=COUNTY_LIST[0],
                        clearable=False,
                        style={"width": "280px"},
                    ),
                ]),
                html.Div([
                    html.Label("Metric Group", style={"fontWeight": "bold", "display": "block", "marginBottom": "6px"}),
                    dcc.Dropdown(
                        id="group_dropdown",
                        options=[{"label": g.title(), "value": g} for g in GROUP_OPTIONS],
                        value="labor",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                ]),
            ],
        ),

        # Loading spinner wraps the graph so users see feedback during API calls
        dcc.Loading(
            id="loading",
            type="circle",
            children=dcc.Graph(id="metrics_graph"),
        ),

        # Subtle data freshness note
        html.P(
            id="data_note",
            style={"color": "#999", "fontSize": "12px", "marginTop": "8px"},
        ),
    ],
)


# ---------------------------------------- #
# Callback                                 #
# ---------------------------------------- #

@app.callback(
    Output("metrics_graph", "figure"),
    Output("data_note", "children"),
    Input("county_dropdown", "value"),
    Input("group_dropdown", "value"),
)
def update_graph(county_pretty: str, group_name: str):
    """
    Fetch the appropriate data for the selected county and group,
    then return a Plotly figure.

    - Labor group → BLS API (cached 30 days)
    - Housing / Economy groups → FRED API (cached 30 days)
    """
    try:
        if group_name == "labor":
            df = get_labor_data_for_county(county_pretty)
        else:
            df = get_fred_data_for_county(county_pretty, group_name)

        fig = build_figure(df, county_pretty, group_name)

        # Show when data was last fetched (approximated)
        note = (
            f"Data source: {'BLS' if group_name == 'labor' else 'FRED'} | "
            f"Auto-refreshes every 30 days"
        )
        return fig, note

    except Exception as e:
        print(f"[ERROR] Failed to build figure for {county_pretty} / {group_name}: {e}")
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            x=0.5, y=0.5, xref="paper", yref="paper",
            text="An error occurred loading data. Please try again shortly.",
            showarrow=False, font=dict(size=14),
        )
        empty_fig.update_layout(height=300)
        return empty_fig, "Data temporarily unavailable."


# ---------------------------------------- #
# Run                                      #
# ---------------------------------------- #

if __name__ == "__main__":
    # debug=True enables hot-reloading when editing the file locally.
    # Render sets PORT automatically; locally it defaults to 8050.
    port = int(os.getenv("PORT", 8050))
    app.run(debug=True, port=port)