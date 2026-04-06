import os
import glob
import requests
import pandas as pd
import plotly.express as px

# ---- 1. Paths ----
script_dir = os.path.dirname(os.path.abspath(__file__))
merged_dir = os.path.join(script_dir, "bls_csv_outputs", "county_data", "merged")
output_dir = os.path.join(script_dir, "choropleth_map_outputs")
os.makedirs(output_dir, exist_ok=True)

# ---- 2. Load GEOJSON ----
url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
counties = requests.get(url).json()

# ---- 3. Read CSVs ----
files = glob.glob(os.path.join(merged_dir, "*_all_metrics.csv"))
all_rows = []

for f in files:
    county = os.path.basename(f).replace("_all_metrics.csv", "")
    df = pd.read_csv(f)

    date_col = [c for c in df.columns if "date" in c.lower()][0]
    latest = df.iloc[-1].copy()
    latest["County"] = county
    latest["Date"] = df[date_col].iloc[-1]

    all_rows.append(latest)

md = pd.DataFrame(all_rows)

# ---- 4. FIPS Mapping ----
county_to_fips = {
    "allegany": "24001",
    "anne_arundel": "24003",
    "baltimore": "24005",
    "baltimore_city": "24510",
    "calvert": "24009",
    "caroline": "24011",
    "carroll": "24013",
    "cecil": "24015",
    "charles": "24017",
    "dorchester": "24019",
    "frederick": "24021",
    "garrett": "24023",
    "harford": "24025",
    "howard": "24027",
    "kent": "24029",
    "montgomery": "24031",
    "prince_georges": "24033",
    "queen_annes": "24035",
    "somerset": "24039",
    "st_marys": "24037",
    "talbot": "24041",
    "washington": "24043",
    "wicomico": "24045",
    "worcester": "24047"
}

md["fips"] = md["County"].map(county_to_fips).astype(str).str.zfill(5)

# ---- 5. Pretty County Names and Dates ----
county_pretty = {
    "allegany": "Allegany County",
    "anne_arundel": "Anne Arundel County",
    "baltimore": "Baltimore County",
    "baltimore_city": "Baltimore City",
    "calvert": "Calvert County",
    "caroline": "Caroline County",
    "carroll": "Carroll County",
    "cecil": "Cecil County",
    "charles": "Charles County",
    "dorchester": "Dorchester County",
    "frederick": "Frederick County",
    "garrett": "Garrett County",
    "harford": "Harford County",
    "howard": "Howard County",
    "kent": "Kent County",
    "montgomery": "Montgomery County",
    "prince_georges": "Prince George's County",
    "queen_annes": "Queen Anne's County",
    "somerset": "Somerset County",
    "st_marys": "St. Mary's County",
    "talbot": "Talbot County",
    "washington": "Washington County",
    "wicomico": "Wicomico County",
    "worcester": "Worcester County"
}

md["PrettyCounty"] = md["County"].map(county_pretty)
md["PrettyDate"] = pd.to_datetime(md["Date"]).dt.strftime("%B %d, %Y")
latest_date = pd.to_datetime(md["Date"]).max()
title_date = latest_date.strftime("%B %Y")

# ---- 6. Metric colors and formatting ----
metric_colors = {
    "Employment": "Blues",
    "Labor Force": "Greens",
    "Unemployment Count": "Reds",
    "Unemployment Rate": "Purples"
}

def format_metric_value(metric_name):
    m = metric_name.lower()
    if "rate" in m:
        return "%{customdata[1]:.2f}%"
    elif "wage" in m or "earn" in m:
        return "$%{customdata[1]:,.0f}"
    else:
        return "%{customdata[1]:,}"

# ---- 7. Generate and save HTML maps ----
def make_md_map(metric, colorscale):
    fig = px.choropleth_mapbox(
        md,
        geojson=counties,
        locations="fips",
        color=metric,
        color_continuous_scale=colorscale,
        mapbox_style="open-street-map",
        center={"lat": 39.0, "lon": -76.7},
        zoom=6.7,
        opacity=0.8,
        hover_data=["PrettyCounty", metric, "PrettyDate"],
        labels={metric: metric.replace('_', ' ').title()}
    )

    # Customize hover template
    value_format = format_metric_value(metric)
    fig.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            f"{metric.replace('_',' ').title()}: {value_format}<br>"
            "Date: %{customdata[2]}<br>"
            "<extra></extra>"
        )
    )

    fig.update_layout(
        title=f"Maryland County {metric.replace('_', ' ').title()} — {title_date}",
        margin={"r":0, "t":40, "l":0, "b":0}
    )

    # Save HTML
    filename = f"{metric.replace(' ','_')}_choropleth.html"
    output_path = os.path.join(output_dir, filename)
    fig.write_html(output_path)

    # Print relative path
    relative_path = os.path.relpath(output_path, start=os.getcwd())
    print(f"[INFO] Saved {filename} at {relative_path}\n")

# ---- 8. Loop through metrics ----
for metric, colorscale in metric_colors.items():
    make_md_map(metric, colorscale)
