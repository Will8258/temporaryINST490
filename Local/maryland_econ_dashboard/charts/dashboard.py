from dash import Dash, html, dcc, Input, Output, ctx, ALL, no_update
import plotly.graph_objects as go
import requests
import pandas as pd
import os
from datetime import datetime

app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server

FRED_API_KEY = os.getenv("FRED_API_KEY", "9e5d944924918a26660adf4a492e447e")
BLS_API_KEY = os.getenv("BLS_API_KEY", "6ce4168cdb20494a8ce7fc2f05baf9bc")
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "59cba10d8a5da536fc06b59dd2d629f2506a477bb122e829694aae00")

FORECLOSURE_API_URL = "https://opendata.maryland.gov/resource/w3bc-8mnv.json"

# -------------------------
# STATEWIDE SERIES
# -------------------------

LABOR_SERIES = {
    "Unemployment Rate": "Unemployment Rate in Maryland",
    "Unemployed Persons": "Unemployed Persons in Maryland",
    "Labor Force Participation Rate": "Labor Force Participation Rate for Maryland",
    "Employment": "Employment Level for Maryland",
    "Initial Claims": "Initial Claims in Maryland",
    "Civilian Labor Force": "Civilian Labor Force in Maryland",
    "Discouraged Workers": "Not in Labor Force: Discouraged Workers for Maryland",
    "Average Hourly Earnings": "Average Hourly Earnings of All Employees: Total Private in Maryland",
}

HOUSING_SERIES = {
    "New Private Housing Units by Building Permits": "New Private Housing Units Authorized by Building Permits for Maryland",
    "Housing Inventory Median Listing Price": "Housing Inventory: Median Listing Price in Maryland",
    "Housing Inventory Active Listing Count": "Housing Inventory: Active Listing Count in Maryland",
    "Zillow Home Value Index": "Zillow Home Value Index (ZHVI) for All Homes Including Single-Family Residences, Condos, and CO-OPs in Maryland",
    "All Transaction House Price Index": "All-Transactions House Price Index for Maryland",
    "Foreclosures": "foreclosures",
}

ECONOMIC_SERIES = {
    "Real GDP": "MDRGSP",
    "Resident Population": "MDPOP",
    "Real Median Income": "MEHOINUSMDA672N",
    "Poverty Rate": "PPAAMD24000A156NCEN",
    "Business Applications EIN Filings": "BABATOTALSAMD",
}

# -------------------------
# COUNTY CONFIG
# -------------------------

COUNTIES = [
    "Allegany", "Anne Arundel", "Baltimore", "Baltimore City",
    "Calvert", "Caroline", "Carroll", "Cecil", "Charles",
    "Dorchester", "Frederick", "Garrett", "Harford",
    "Howard", "Kent", "Montgomery", "Prince George's",
    "Queen Anne's", "Somerset", "St. Mary's",
    "Talbot", "Washington", "Wicomico", "Worcester"
]

COUNTY_FIPS = {
    "Allegany": "001",
    "Anne Arundel": "003",
    "Baltimore": "005",
    "Baltimore City": "510",
    "Calvert": "009",
    "Caroline": "011",
    "Carroll": "013",
    "Cecil": "015",
    "Charles": "017",
    "Dorchester": "019",
    "Frederick": "021",
    "Garrett": "023",
    "Harford": "025",
    "Howard": "027",
    "Kent": "029",
    "Montgomery": "031",
    "Prince George's": "033",
    "Queen Anne's": "035",
    "St. Mary's": "037",
    "Somerset": "039",
    "Talbot": "041",
    "Washington": "043",
    "Wicomico": "045",
    "Worcester": "047",
}

COUNTY_METRICS = {
    "Labor": [
        "Unemployment Rate",
        "Unemployed Persons",
        "Employment",
        "Civilian Labor Force"
    ],
    "Housing": [
        "Median Listing Price",
        "Active Listing Count",
        "Building Permits"
    ],
    "Economic": [
        "Resident Population",
        "Median Household Income",
        "Poverty Rate"
    ],
    "Foreclosures": [
        "Notice of Intent",
        "Notice of Foreclosure",
        "Foreclosure Property Registration"
    ]
}

CATEGORIES = {
    "labor": {"title": "MARYLAND LABOR STATISTICS:", "buttons": list(LABOR_SERIES.keys())},
    "housing": {"title": "MARYLAND HOUSING STATISTICS:", "buttons": list(HOUSING_SERIES.keys())},
    "economic": {"title": "MARYLAND ECONOMIC STATISTICS:", "buttons": list(ECONOMIC_SERIES.keys())},
    "county": {
        "title": "MARYLAND COUNTY STATISTICS:",
        "buttons": [
            "All Counties Labor Statistics",
            "All Counties Housing Statistics",
            "All Counties Economic Statistics",
            "All Counties Foreclosure Statistics"
        ]
    },
}

# -------------------------
# FRED HELPERS
# -------------------------

def fred_search_series_id(title):
    url = "https://api.stlouisfed.org/fred/series/search"
    params = {
        "search_text": title,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "limit": 10
    }

    res = requests.get(url, params=params, timeout=30)
    res.raise_for_status()
    results = res.json().get("seriess", [])

    if not results:
        raise ValueError(f"No FRED series found for {title}")

    for s in results:
        if s["title"].lower().strip() == title.lower().strip():
            return s["id"]

    return results[0]["id"]


def fetch_fred(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }

    res = requests.get(url, params=params, timeout=30)
    res.raise_for_status()

    df = pd.DataFrame(res.json()["observations"])
    df = df[df["value"] != "."].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df[["date", "value"]].dropna().sort_values("date")


# -------------------------
# BLS COUNTY LABOR
# -------------------------

def bls_county_series_id(county, metric):
    fips = "24" + COUNTY_FIPS[county]

    measure_codes = {
        "Unemployment Rate": "03",
        "Unemployed Persons": "04",
        "Employment": "05",
        "Civilian Labor Force": "06"
    }

    measure = measure_codes[metric]

    return f"LAUCN{fips}00000000{measure}"


def fetch_bls_series(series_id):
    current_year = datetime.now().year
    start_year = current_year - 10

    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    payload = {
        "seriesid": [series_id],
        "startyear": str(start_year),
        "endyear": str(current_year)
    }

    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY

    res = requests.post(url, json=payload, timeout=30)
    res.raise_for_status()
    data = res.json()

    series_data = data["Results"]["series"][0]["data"]

    rows = []

    for item in series_data:
        period = item.get("period")
        raw_value = item.get("value")

        if not period or not period.startswith("M") or period == "M13":
            continue

        # Skip blanks, dashes, suppressed values
        if raw_value in ["", "-", ".", None]:
            continue

        try:
            value = float(str(raw_value).replace(",", ""))
        except ValueError:
            continue

        month = int(period.replace("M", ""))
        year = int(item["year"])

        rows.append({
            "date": pd.Timestamp(year=year, month=month, day=1),
            "value": value
        })

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"No usable BLS data returned for {series_id}")

    return df.sort_values("date")


# -------------------------
# CENSUS COUNTY ECONOMIC
# -------------------------

def fetch_county_census_economic(county, metric):
    county_fips = COUNTY_FIPS[county]
    rows = []

    current_year = datetime.now().year
    years = list(range(current_year - 12, current_year))

    variable_map = {
        "Resident Population": "B01003_001E",
        "Median Household Income": "B19013_001E",
    }

    for year in years:
        try:
            if metric == "Poverty Rate":
                get_vars = "NAME,B17001_002E,B17001_001E"
            else:
                get_vars = f"NAME,{variable_map[metric]}"

            url = f"https://api.census.gov/data/{year}/acs/acs5"
            params = {
                "get": get_vars,
                "for": f"county:{county_fips}",
                "in": "state:24"
            }

            if CENSUS_API_KEY:
                params["key"] = CENSUS_API_KEY

            res = requests.get(url, params=params, timeout=30)

            if res.status_code != 200:
                continue

            data = res.json()

            if len(data) < 2:
                continue

            header = data[0]
            values = data[1]
            row = dict(zip(header, values))

            if metric == "Poverty Rate":
                below = pd.to_numeric(row.get("B17001_002E"), errors="coerce")
                total = pd.to_numeric(row.get("B17001_001E"), errors="coerce")

                if pd.isna(below) or pd.isna(total) or total == 0:
                    continue

                value = (below / total) * 100
            else:
                value = pd.to_numeric(row.get(variable_map[metric]), errors="coerce")

                if pd.isna(value):
                    continue

            rows.append({
                "date": pd.Timestamp(year=year, month=1, day=1),
                "value": float(value)
            })

        except Exception:
            continue

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"No Census data found for {county} {metric}")

    return df.sort_values("date")


# -------------------------
# MARYLAND OPEN DATA FORECLOSURES
# -------------------------

def fetch_foreclosures_raw():
    params = {"$limit": 50000, "$order": "date ASC"}
    res = requests.get(FORECLOSURE_API_URL, params=params, timeout=30)
    res.raise_for_status()

    df = pd.DataFrame(res.json())
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    return df


def fetch_foreclosures():
    df = fetch_foreclosures_raw()

    ignored = {"date", "type", ":id", ":created_at", ":updated_at", ":version"}
    county_cols = [c for c in df.columns if c not in ignored]

    for c in county_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["statewide_total"] = df[county_cols].sum(axis=1)

    return df[["date", "type", "statewide_total"]].sort_values("date")


def find_foreclosure_county_column(df, county):
    clean_target_options = [
        county.lower().replace(" ", "_").replace(".", "").replace("'", "").replace("-", "_"),
        f"{county.lower()}_county".replace(" ", "_").replace(".", "").replace("'", "").replace("-", "_"),
        county.lower().replace(" ", "").replace(".", "").replace("'", "").replace("-", "")
    ]

    if county == "Baltimore City":
        clean_target_options += ["baltimore_city", "baltimorecity"]

    for col in df.columns:
        clean_col = col.lower().replace(".", "").replace("'", "").replace("-", "_")

        if clean_col in clean_target_options:
            return col

    for col in df.columns:
        clean_col = col.lower().replace(".", "").replace("'", "").replace("-", "_")

        if county.lower().replace("'", "").replace(".", "") in clean_col.replace("_", " "):
            return col

    raise ValueError(f"Could not find foreclosure column for {county}")


def county_foreclosure_chart(county, metric):
    df = fetch_foreclosures_raw()
    county_col = find_foreclosure_county_column(df, county)

    df[county_col] = pd.to_numeric(df[county_col], errors="coerce").fillna(0)

    # Match foreclosure type flexibly
    metric_clean = metric.lower().strip()

    if metric_clean == "notice of intent":
        df_m = df[df["type"].str.lower().str.contains("intent", na=False)]
    elif metric_clean == "notice of foreclosure":
        df_m = df[df["type"].str.lower().str.contains("foreclosure", na=False)]
        df_m = df_m[~df_m["type"].str.lower().str.contains("property", na=False)]
    elif metric_clean == "foreclosure property registration":
        df_m = df[df["type"].str.lower().str.contains("property", na=False)]
    else:
        df_m = df[df["type"].str.lower() == metric_clean]

    df_m = df_m.sort_values("date")

    if df_m.empty:
        return empty_chart(f"No foreclosure data found for {county} — {metric}")

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_m["date"],
        y=df_m[county_col],
        mode="lines+markers",
        line=dict(color="#4a5878", width=3),
        marker=dict(size=5),
        name=metric
    ))

    fig.update_layout(
        title=f"{county} County — {metric}",
        xaxis_title="Date",
        yaxis_title="Foreclosure Count",
        template="plotly_white",
        height=500,
        margin=dict(l=70, r=30, t=70, b=60)
    )

    return fig


# -------------------------
# CHART HELPERS
# -------------------------

def line_chart(title, df, y_title):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["value"],
        mode="lines+markers",
        line=dict(color="#4a5878", width=3),
        marker=dict(size=5),
        name=title
    ))

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=y_title,
        template="plotly_white",
        height=500,
        margin=dict(l=70, r=30, t=70, b=60)
    )

    return fig


def bar_chart(title, df, y_title):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["date"],
        y=df["value"],
        marker=dict(color="#2f568d", line=dict(color="black", width=0.3)),
        name=title
    ))

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=y_title,
        template="plotly_white",
        height=500,
        margin=dict(l=70, r=30, t=70, b=60)
    )

    return fig


def unemployment_claims_chart(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["value"],
        mode="lines",
        line=dict(color="crimson", width=3),
        name="Initial Claims"
    ))

    fig.update_layout(
        title="Initial Unemployment Claims — Maryland",
        xaxis_title="Date",
        yaxis_title="Claims",
        template="plotly_white",
        height=500,
        margin=dict(l=70, r=30, t=70, b=60),
        xaxis=dict(rangeslider=dict(visible=True), type="date")
    )

    return fig


def foreclosure_chart():
    df = fetch_foreclosures()
    fig = go.Figure()

    for t in sorted(df["type"].dropna().unique()):
        df_t = df[df["type"] == t].sort_values("date")
        fig.add_trace(go.Scatter(
            x=df_t["date"],
            y=df_t["statewide_total"],
            mode="lines+markers",
            name=t
        ))

    fig.update_layout(
        title="Maryland Foreclosures",
        xaxis_title="Date",
        yaxis_title="Statewide Total",
        template="plotly_white",
        height=500,
        margin=dict(l=70, r=30, t=70, b=60)
    )

    return fig


def empty_chart(message="Select an indicator"):
    fig = go.Figure()
    fig.update_layout(
        title=message,
        template="plotly_white",
        height=500
    )
    return fig


def county_location_name(county):
    if county == "Baltimore City":
        return "Baltimore city, MD"

    return f"{county} County, MD"

def get_county_economic_fred_titles(county, metric):
    place = county_location_name(county)

    if metric == "Resident Population":
        return [
            f"Resident Population in {place}",
            f"Population Estimate, Total for {place}"
        ]

    if metric == "Median Household Income":
        return [
            f"Estimate of Median Household Income for {place}",
            f"Median Household Income in {place}"
        ]

    if metric == "Poverty Rate":
        return [
            f"Estimated Percent of People of All Ages in Poverty for {place}",
            f"Percent of Population Below the Poverty Level in {place}"
        ]

    return []


def get_county_fred_title(county, section, metric):
    place = county_location_name(county)

    titles = {
        "Housing": {
            "Median Listing Price": f"Housing Inventory: Median Listing Price in {place}",
            "Active Listing Count": f"Housing Inventory: Active Listing Count in {place}",
            "Building Permits": f"New Private Housing Units Authorized by Building Permits for {place}",
        }
    }

    return titles[section][metric]


def get_county_chart(county, section, metric):
    try:
        if section == "Labor":
            series_id = bls_county_series_id(county, metric)
            df = fetch_bls_series(series_id)

            if metric == "Unemployment Rate":
                return line_chart(f"{county} County — {metric}", df, "Percent")

            return line_chart(f"{county} County — {metric}", df, "Persons")

        if section == "Housing":
            title = get_county_fred_title(county, section, metric)
            series_id = fred_search_series_id(title)
            df = fetch_fred(series_id)

            if metric == "Median Listing Price":
                return line_chart(f"{county} County — {metric}", df, "Dollars")

            return line_chart(f"{county} County — {metric}", df, "Count")

        if section == "Economic":
            df = fetch_county_census_economic(county, metric)

            if metric == "Poverty Rate":
                return line_chart(f"{county} County — {metric}", df, "Percent")

            if metric == "Median Household Income":
                return line_chart(f"{county} County — {metric}", df, "Dollars")

            return line_chart(f"{county} County — {metric}", df, "Persons")

        if section == "Foreclosures":
            return county_foreclosure_chart(county, metric)

    except Exception as e:
        return empty_chart(f"Error loading {county} {metric}: {e}")

    return empty_chart()


def get_real_chart(category, subcategory):
    try:
        if category == "labor":
            title = LABOR_SERIES[subcategory]
            series_id = fred_search_series_id(title)
            df = fetch_fred(series_id)

            if subcategory == "Initial Claims":
                return unemployment_claims_chart(df)

            if subcategory in ["Unemployment Rate", "Labor Force Participation Rate"]:
                return line_chart(subcategory, df, "Percent")

            if subcategory == "Average Hourly Earnings":
                return line_chart(subcategory, df, "Dollars per Hour")

            return line_chart(subcategory, df, "Persons")

        if category == "housing":
            if subcategory == "Foreclosures":
                return foreclosure_chart()

            title = HOUSING_SERIES[subcategory]
            series_id = fred_search_series_id(title)
            df = fetch_fred(series_id)

            if subcategory in [
                "New Private Housing Units by Building Permits",
                "Housing Inventory Active Listing Count"
            ]:
                return bar_chart(subcategory, df, "Count")

            if subcategory in [
                "Housing Inventory Median Listing Price",
                "Zillow Home Value Index"
            ]:
                return line_chart(subcategory, df, "Dollars")

            return line_chart(subcategory, df, "Index")

        if category == "economic":
            series_id = ECONOMIC_SERIES[subcategory]
            df = fetch_fred(series_id)

            if subcategory == "Real GDP":
                return line_chart(subcategory, df, "Millions of Chained Dollars")

            if subcategory == "Resident Population":
                return line_chart(subcategory, df, "Thousands of Persons")

            if subcategory == "Real Median Income":
                return line_chart(subcategory, df, "Dollars")

            if subcategory == "Poverty Rate":
                return line_chart(subcategory, df, "Percent")

            if subcategory == "Business Applications EIN Filings":
                return bar_chart(subcategory, df, "Applications")

    except Exception as e:
        return empty_chart(f"Error loading chart: {e}")

    return empty_chart()


# -------------------------
# UI HELPERS
# -------------------------

def category_button(label, category, selected=False):
    return html.Button(
        label,
        id={"type": "category-btn", "category": category},
        n_clicks=0,
        style={
            "width": "220px",
            "height": "75px",
            "margin": "12px",
            "fontWeight": "bold",
            "backgroundColor": "#8f8f8f" if selected else "white",
            "border": "1px solid #444",
            "cursor": "pointer"
        }
    )


def sub_button(label, selected=False):
    return html.Button(
        label,
        id={"type": "sub-btn", "name": label},
        n_clicks=0,
        style={
            "width": "280px",
            "padding": "12px",
            "marginBottom": "8px",
            "fontWeight": "bold",
            "backgroundColor": "#8f8f8f" if selected else "white",
            "color": "black",
            "border": "1px solid #444",
            "cursor": "pointer"
        }
    )


def county_button(county, selected=False):
    return html.Button(
        county,
        id={"type": "county-btn", "name": county},
        n_clicks=0,
        style={
            "width": "170px",
            "padding": "10px",
            "margin": "6px",
            "fontWeight": "bold",
            "backgroundColor": "#8f8f8f" if selected else "white",
            "border": "1px solid #444",
            "cursor": "pointer"
        }
    )


def county_section_button(label, selected=False):
    return html.Button(
        label,
        id={"type": "county-section-btn", "name": label},
        n_clicks=0,
        style={
            "width": "220px",
            "height": "55px",
            "margin": "8px",
            "fontWeight": "bold",
            "backgroundColor": "#8f8f8f" if selected else "white",
            "border": "1px solid #444",
            "cursor": "pointer"
        }
    )


def county_metric_button(label, selected=False):
    return html.Button(
        label,
        id={"type": "county-metric-btn", "name": label},
        n_clicks=0,
        style={
            "width": "240px",
            "padding": "10px",
            "marginBottom": "8px",
            "fontWeight": "bold",
            "backgroundColor": "#8f8f8f" if selected else "white",
            "border": "1px solid #444",
            "cursor": "pointer"
        }
    )


def map_placeholder(title="Interactive Map of Maryland Counties"):
    return html.Div(
        title,
        style={
            "height": "280px",
            "border": "2px solid #222",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center",
            "fontSize": "22px",
            "fontWeight": "bold",
            "color": "#4a5878",
            "marginBottom": "25px",
            "backgroundColor": "#f7f7f7"
        }
    )


# -------------------------
# LAYOUT
# -------------------------

app.layout = html.Div([
    dcc.Store(id="selected-category", data=None),
    dcc.Store(id="selected-subcategory", data=None),
    dcc.Store(id="selected-county", data=None),
    dcc.Store(id="selected-county-section", data="Labor"),
    dcc.Store(id="selected-county-metric", data=None),

    html.Div(style={
        "height": "28px",
        "background": "linear-gradient(to right, #2f568d, #00a6d6)"
    }),

    html.Div([
        html.Div("NCSG", style={
            "fontSize": "46px",
            "fontWeight": "bold",
            "color": "#2f568d",
            "marginRight": "60px"
        }),
        html.H1("iConsultancy Accomplishments", style={
            "fontSize": "58px",
            "color": "#2f568d",
            "margin": "20px 0"
        })
    ], style={
        "display": "flex",
        "alignItems": "center",
        "padding": "0 40px"
    }),

    html.Button(
        "Back",
        id="back-btn",
        n_clicks=0,
        style={
            "display": "none",
            "position": "absolute",
            "right": "90px",
            "top": "185px",
            "backgroundColor": "#222",
            "color": "white",
            "padding": "10px 28px",
            "border": "none",
            "fontWeight": "bold",
            "cursor": "pointer",
            "zIndex": "10"
        }
    ),

    html.Div(id="page-content", style={"padding": "30px 70px"})
], style={
    "fontFamily": "Arial, sans-serif",
    "backgroundColor": "white",
    "minHeight": "100vh"
})


# -------------------------
# RENDER PAGES
# -------------------------

@app.callback(
    Output("page-content", "children"),
    Input("selected-category", "data"),
    Input("selected-subcategory", "data"),
    Input("selected-county", "data"),
    Input("selected-county-section", "data"),
    Input("selected-county-metric", "data")
)
def render_page(category, subcategory, county, county_section, county_metric):
    if category is None:
        return html.Div([
            html.Div([
                html.Div("Maryland Indicators Dashboard", style={
                    "backgroundColor": "#424d6b",
                    "color": "white",
                    "fontSize": "30px",
                    "padding": "35px",
                    "textAlign": "center",
                    "width": "360px",
                    "margin": "0 auto 30px auto"
                }),

                html.P(
                    "Explore key statewide and county-level data on labor, housing, "
                    "and economic conditions across Maryland through this interactive dashboard. "
                    "All data is updated automatically from trusted public sources.",
                    style={
                        "fontWeight": "bold",
                        "maxWidth": "760px",
                        "margin": "20px auto",
                        "textAlign": "left"
                    }
                ),

                html.Div([
                    category_button("Maryland Labor Statistics", "labor"),
                    category_button("Maryland Housing Statistics", "housing"),
                    category_button("Maryland Economic Statistics", "economic"),
                    category_button("Maryland County Statistics", "county")
                ], style={
                    "display": "flex",
                    "justifyContent": "center",
                    "flexWrap": "wrap"
                })
            ], style={
                "border": "2px solid #2f568d",
                "boxShadow": "0 4px 10px rgba(0,0,0,0.25)",
                "padding": "55px",
                "maxWidth": "950px",
                "margin": "0 auto"
            })
        ])

    if category == "county":
        info = CATEGORIES["county"]

        if subcategory not in info["buttons"]:
            selected_subcategory = info["buttons"][0]
        else:
            selected_subcategory = subcategory

        if county is None:
            return html.Div([
                html.Div([
                    html.H3("MARYLAND COUNTY STATISTICS:", style={
                        "textDecoration": "underline",
                        "marginBottom": "20px"
                    }),

                    html.Div([
                        category_button("Maryland Labor Statistics", "labor"),
                        category_button("Maryland Housing Statistics", "housing"),
                        category_button("Maryland Economic Statistics", "economic"),
                        category_button("Maryland County Statistics", "county", selected=True)
                    ], style={
                        "display": "flex",
                        "justifyContent": "center",
                        "marginBottom": "25px",
                        "flexWrap": "wrap"
                    }),

                    map_placeholder(),

                    html.Div([
                        sub_button("All Counties Labor Statistics", selected=(selected_subcategory == "All Counties Labor Statistics")),
                        sub_button("All Counties Housing Statistics", selected=(selected_subcategory == "All Counties Housing Statistics")),
                        sub_button("All Counties Economic Statistics", selected=(selected_subcategory == "All Counties Economic Statistics")),
                        sub_button("All Counties Foreclosure Statistics", selected=(selected_subcategory == "All Counties Foreclosure Statistics")),
                    ], style={
                        "display": "flex",
                        "justifyContent": "center",
                        "gap": "18px",
                        "flexWrap": "wrap",
                        "marginBottom": "25px"
                    }),

                    html.H4(selected_subcategory, style={"marginTop": "20px"}),

                    html.Div([
                        county_button(c) for c in COUNTIES
                    ], style={
                        "display": "flex",
                        "flexWrap": "wrap",
                        "justifyContent": "center",
                        "marginTop": "15px"
                    }),

                    html.P(
                        "Click a county above to open that county's labor, housing, economic, and foreclosure dashboard.",
                        style={"textAlign": "center", "fontWeight": "bold", "marginTop": "20px"}
                    )
                ], style={
                    "border": "2px solid #2f568d",
                    "boxShadow": "0 4px 10px rgba(0,0,0,0.25)",
                    "padding": "25px",
                    "maxWidth": "1150px",
                    "margin": "0 auto",
                    "position": "relative"
                })
            ])

        section = county_section or "Labor"

        if county_metric not in COUNTY_METRICS.get(section, []):
            selected_metric = COUNTY_METRICS[section][0]
        else:
            selected_metric = county_metric

        return html.Div([
            html.Div([
                html.H3(f"{county.upper()} COUNTY STATISTICS:", style={
                    "textDecoration": "underline",
                    "marginBottom": "20px"
                }),

                html.Div([
                    category_button("Maryland Labor Statistics", "labor"),
                    category_button("Maryland Housing Statistics", "housing"),
                    category_button("Maryland Economic Statistics", "economic"),
                    category_button("Maryland County Statistics", "county", selected=True)
                ], style={
                    "display": "flex",
                    "justifyContent": "center",
                    "marginBottom": "25px",
                    "flexWrap": "wrap"
                }),

                map_placeholder(f"Interactive Map Placeholder — {county} County Selected"),

                html.Div([
                    county_section_button(f"{county} Labor Stats", selected=(section == "Labor")),
                    county_section_button(f"{county} Housing Stats", selected=(section == "Housing")),
                    county_section_button(f"{county} Economic Stats", selected=(section == "Economic")),
                    county_section_button(f"{county} Foreclosures", selected=(section == "Foreclosures")),
                ], style={
                    "display": "flex",
                    "justifyContent": "center",
                    "marginBottom": "20px",
                    "flexWrap": "wrap"
                }),

                html.Div([
                    html.Div([
                        html.H5(f"{section.upper()} STATISTICS"),

                        html.Button(
                            "Choose Different County",
                            id={"type": "clear-county-btn", "name": "clear"},
                            n_clicks=0,
                            style={
                                "width": "240px",
                                "padding": "10px",
                                "marginBottom": "12px",
                                "fontWeight": "bold",
                                "backgroundColor": "white",
                                "border": "1px solid #444",
                                "cursor": "pointer"
                            }
                        ),

                        *[
                            county_metric_button(metric, selected=(metric == selected_metric))
                            for metric in COUNTY_METRICS[section]
                        ]
                    ], style={
                        "width": "280px",
                        "paddingRight": "25px"
                    }),

                    html.Div([
                        dcc.Loading(
                            dcc.Graph(
                                figure=get_county_chart(county, section, selected_metric),
                                style={"height": "520px"}
                            )
                        )
                    ], style={"flex": "1"})
                ], style={"display": "flex"})
            ], style={
                "border": "2px solid #2f568d",
                "boxShadow": "0 4px 10px rgba(0,0,0,0.25)",
                "padding": "25px",
                "maxWidth": "1150px",
                "margin": "0 auto",
                "position": "relative"
            })
        ])

    info = CATEGORIES[category]

    if subcategory not in info["buttons"]:
        selected = info["buttons"][0]
    else:
        selected = subcategory

    return html.Div([
        html.Div([
            html.H3(info["title"], style={
                "textDecoration": "underline",
                "marginBottom": "20px"
            }),

            html.Div([
                category_button("Maryland Labor Statistics", "labor", selected=(category == "labor")),
                category_button("Maryland Housing Statistics", "housing", selected=(category == "housing")),
                category_button("Maryland Economic Statistics", "economic", selected=(category == "economic")),
                category_button("Maryland County Statistics", "county", selected=(category == "county"))
            ], style={
                "display": "flex",
                "justifyContent": "center",
                "marginBottom": "25px",
                "flexWrap": "wrap"
            }),

            html.Div([
                html.Div([
                    html.H5(info["title"].replace("MARYLAND ", "").replace(":", "")),
                    *[sub_button(label, selected=(label == selected)) for label in info["buttons"]]
                ], style={
                    "width": "310px",
                    "paddingRight": "25px"
                }),

                html.Div([
                    html.Div(
                        "Instructions: SELECT an indicator to view.",
                        style={
                            "color": "red",
                            "fontWeight": "bold",
                            "fontSize": "12px",
                            "marginBottom": "8px"
                        }
                    ),
                    dcc.Loading(
                        dcc.Graph(
                            id="main-chart",
                            figure=get_real_chart(category, selected),
                            style={"height": "520px"}
                        )
                    )
                ], style={"flex": "1"})
            ], style={"display": "flex"})
        ], style={
            "border": "2px solid #2f568d",
            "boxShadow": "0 4px 10px rgba(0,0,0,0.25)",
            "padding": "25px",
            "maxWidth": "1150px",
            "margin": "0 auto",
            "position": "relative"
        })
    ])


# -------------------------
# CALLBACKS
# -------------------------

@app.callback(
    Output("selected-category", "data"),
    Output("selected-subcategory", "data", allow_duplicate=True),
    Output("selected-county", "data", allow_duplicate=True),
    Output("selected-county-section", "data", allow_duplicate=True),
    Output("selected-county-metric", "data", allow_duplicate=True),
    Input({"type": "category-btn", "category": ALL}, "n_clicks"),
    Input("back-btn", "n_clicks"),
    prevent_initial_call=True
)
def update_category(category_clicks, back_clicks):
    triggered = ctx.triggered_id
    triggered_value = ctx.triggered[0]["value"]

    # Ignore fake triggers from Dash rebuilding layout
    if triggered_value in [None, 0, []]:
        return no_update, no_update, no_update, no_update, no_update

    if triggered == "back-btn":
        return None, None, None, "Labor", None

    if isinstance(triggered, dict) and triggered.get("type") == "category-btn":
        return triggered["category"], None, None, "Labor", None

    return no_update, no_update, no_update, no_update, no_update


@app.callback(
    Output("selected-subcategory", "data"),
    Input({"type": "sub-btn", "name": ALL}, "n_clicks"),
    prevent_initial_call=True
)
def update_subcategory(sub_clicks):
    if not sub_clicks or max(sub_clicks) == 0:
        return no_update

    if isinstance(ctx.triggered_id, dict):
        return ctx.triggered_id["name"]

    return no_update


@app.callback(
    Output("selected-county", "data"),
    Output("selected-county-section", "data", allow_duplicate=True),
    Output("selected-county-metric", "data", allow_duplicate=True),
    Input({"type": "county-btn", "name": ALL}, "n_clicks"),
    Input({"type": "clear-county-btn", "name": ALL}, "n_clicks"),
    prevent_initial_call=True
)
def update_county(county_clicks, clear_clicks):
    triggered = ctx.triggered_id
    triggered_value = ctx.triggered[0]["value"]

    # Ignore fake triggers from layout refresh
    if triggered_value in [None, 0, []]:
        return no_update, no_update, no_update

    if isinstance(triggered, dict) and triggered.get("type") == "clear-county-btn":
        return None, "Labor", None

    if isinstance(triggered, dict) and triggered.get("type") == "county-btn":
        return triggered["name"], "Labor", None

    return no_update, no_update, no_update


@app.callback(
    Output("selected-county-section", "data"),
    Output("selected-county-metric", "data", allow_duplicate=True),
    Input({"type": "county-section-btn", "name": ALL}, "n_clicks"),
    prevent_initial_call=True
)
def update_county_section(section_clicks):
    if not section_clicks or max(section_clicks) == 0:
        return no_update, no_update

    if isinstance(ctx.triggered_id, dict):
        label = ctx.triggered_id["name"]

        if "Labor" in label:
            return "Labor", None

        if "Housing" in label:
            return "Housing", None

        if "Economic" in label:
            return "Economic", None

        if "Foreclosures" in label:
            return "Foreclosures", None

    return no_update, no_update


@app.callback(
    Output("selected-county-metric", "data"),
    Input({"type": "county-metric-btn", "name": ALL}, "n_clicks"),
    prevent_initial_call=True
)
def update_county_metric(metric_clicks):
    if not metric_clicks or max(metric_clicks) == 0:
        return no_update

    if isinstance(ctx.triggered_id, dict):
        return ctx.triggered_id["name"]

    return no_update


@app.callback(
    Output("back-btn", "style"),
    Input("selected-category", "data")
)
def toggle_back_button(category):
    style = {
        "position": "absolute",
        "right": "90px",
        "top": "185px",
        "backgroundColor": "#222",
        "color": "white",
        "padding": "10px 28px",
        "border": "none",
        "fontWeight": "bold",
        "cursor": "pointer",
        "zIndex": "10"
    }

    style["display"] = "none" if category is None else "block"
    return style


if __name__ == "__main__":
    app.run(debug=True)