# Maryland Data Dashboard (NCSG Project)

**Developed for:** National Center for Smart Growth (NCSG)
**Written by:** Bruce Reyes, Himani Majithia, William Washington, Maham Zaidi, Layan Atiya    
**University of Maryland, College Park**

---

## Overview

The Maryland Data Dashboard is an interactive web application that visualizes Maryland statewide and county-level indicators across four main categories:

- Labor Statistics
- Housing Data
- Economic Indicators
- Foreclosure Trends

The dashboard is built with **Python**, **Dash**, **Plotly**, **Pandas**, and **Requests**. It is deployed on **Render** and embedded into a **WordPress** page using an iframe.

---

## Live Application

| Platform | URL |
|---|---|
| Render Deployment | https://nscg-data-dashboard.onrender.com/ |
| WordPress Page | https://www.umdsmartgrowth.org/inst490-test-page |

---

## Features

- Interactive category navigation for Labor, Housing, Economic, and County dashboards
- Statewide Maryland charts using live public APIs
- County-level pages for Maryland counties
- County labor statistics from the Bureau of Labor Statistics
- County economic indicators from Census SAIPE
- County foreclosure data from Maryland Open Data
- Dynamic Plotly charts based on user selections
- WordPress integration through an embedded Render-hosted Dash app

---

## Data Sources

| Source | Used For |
|---|---|
| FRED | Statewide labor, housing, and economic indicators |
| BLS | County labor statistics through LAUS |
| Census SAIPE | County poverty, income, and poverty count indicators |
| Maryland Open Data Portal | Statewide and county foreclosure data |

---

## Data Source Details

### Federal Reserve Economic Data (FRED)

FRED is used for statewide Maryland indicators, including:

- Real GDP
- Resident population
- Real median income
- Poverty rate
- Business applications / EIN filings
- Statewide housing indicators
- Statewide labor indicators

### Bureau of Labor Statistics (BLS)

BLS is used for county-level labor statistics through Local Area Unemployment Statistics (LAUS), including:

- Unemployment rate
- Unemployed persons
- Employment
- Civilian labor force

### U.S. Census SAIPE API

Census SAIPE is used for county-level economic indicators, including:

- Median household income
- Poverty rate
- People in poverty

> **Important:** SAIPE data is intentionally delayed due to official Census release schedules. SAIPE does **not** provide total resident population.

### Maryland Open Data Portal

Maryland Open Data is used for foreclosure data, including:

- Notice of Intent
- Notice of Foreclosure
- Foreclosure Property Registration

---

## Data Update Frequency

The dashboard updates based on the availability of the source APIs.

| Source | Typical Update Frequency |
|---|---|
| FRED | Varies by series; monthly, quarterly, or annually |
| BLS | Usually monthly |
| Census SAIPE | Annually, with a one- to two-year release delay |
| Maryland Open Data | Varies by dataset update schedule |

Because the dashboard uses live API calls, charts reflect the latest data available from each source.

---

## Architecture

    APIs
      ↓
    Data Fetching
      ↓
    Data Cleaning
      ↓
    Pandas DataFrames
      ↓
    Plotly Figures
      ↓
    Dash UI
      ↓
    Render Deployment
      ↓
    WordPress Embed

---

## Project Structure

    Local/maryland_econ_dashboard/charts/
    │
    ├── dashboard.py          # Main Dash application
    ├── requirements.txt      # Python dependencies
    ├── fred_api.py           # Legacy/supporting FRED API work
    ├── bls_api.py            # Legacy/supporting BLS API work
    ├── socrata_api.py        # Legacy/supporting Maryland Open Data work
    └── ipums_api.py          # Legacy/optional NHGIS work

The current production dashboard is primarily contained in:

    Local/maryland_econ_dashboard/charts/dashboard.py

---

## Main Application File

`dashboard.py` contains:

- API configuration
- Statewide series configuration
- County FIPS mappings
- Data fetching functions
- Data cleaning logic
- Plotly chart functions
- Dash layout
- Dash callbacks
- Render deployment server object

The file must include:

    server = app.server

Render uses this object when running the app with Gunicorn.

---

## Dependencies

The `requirements.txt` file should include:

    dash
    plotly
    pandas
    requests
    gunicorn

If any package is missing, Render may fail during deployment.

---

## Local Setup

### 1. Navigate to the dashboard folder

    cd Local/maryland_econ_dashboard/charts

### 2. Install dependencies

    pip install -r requirements.txt

### 3. Set API keys

API keys should be stored as environment variables.

    export FRED_API_KEY=your_fred_key
    export BLS_API_KEY=your_bls_key
    export CENSUS_API_KEY=your_census_key

The Census SAIPE API can often work without a key, but a valid key may help avoid request limits.

> Do not commit API keys directly to GitHub.

### 4. Run the app locally

    python dashboard.py

Then open:

    http://127.0.0.1:8050

---

## Render Deployment

The dashboard is deployed as a Render Web Service.

### Render Settings

| Setting | Value |
|---|---|
| Environment | Python 3 |
| Branch | main |
| Root Directory | `Local/maryland_econ_dashboard/charts` |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `gunicorn dashboard:server` |

### Environment Variables on Render

Add these in the Render service under **Environment**:

    FRED_API_KEY
    BLS_API_KEY
    CENSUS_API_KEY

After changing the code, commit and push to GitHub. Render can redeploy from the latest commit.

---

## WordPress Integration

The Dash app is hosted on Render and embedded into WordPress using an iframe.

Example iframe:

    <iframe
      src="https://nscg-data-dashboard.onrender.com/"
      width="100%"
      height="1000px"
      style="border:none;">
    </iframe>

If the dashboard appears compressed in WordPress, increase the iframe height or adjust the WordPress page/container settings to allow full-width display.

---

## Key Functions

| Function | Purpose |
|---|---|
| `fetch_fred()` | Fetches FRED time series observations |
| `fred_search_series_id()` | Searches FRED by title and returns the closest matching series ID |
| `fetch_bls_series()` | Fetches BLS LAUS time series data for county labor metrics |
| `fetch_county_saipe()` | Fetches Census SAIPE county economic data |
| `fetch_foreclosures()` | Fetches statewide foreclosure totals from Maryland Open Data |
| `county_foreclosure_chart()` | Builds county-level foreclosure charts |
| `get_real_chart()` | Routes statewide category selections to the correct chart |
| `get_county_chart()` | Routes county, section, and metric selections to the correct county chart |

---

## Dashboard Navigation

The dashboard includes four major sections.

### Labor

Statewide Maryland labor indicators.

### Housing

Statewide Maryland housing indicators.

### Economic

Statewide Maryland economic indicators.

### County

County-level dashboard that allows users to select a Maryland county and view:

- County labor metrics
- County housing metrics
- County economic metrics
- County foreclosure metrics

---

## County-Level Notes

County labor data is pulled from BLS LAUS using county FIPS codes.

County economic data uses Census SAIPE. SAIPE does not provide total resident population, so the dashboard uses available SAIPE indicators such as:

- People in poverty
- Poverty rate
- Median household income

County foreclosure data is pulled from the Maryland Open Data Portal and filtered by county and foreclosure type.

---

## Known Limitations

- Initial page load may be slow because the app relies on live API calls.
- Some APIs may occasionally return errors, missing values, or delayed responses.
- SAIPE data is intentionally delayed and should not be expected to include the current year.
- SAIPE does not provide resident population.
- Some county-level housing indicators may not exist for every county in FRED.
- WordPress iframe display may need height or width adjustments.

---

## Troubleshooting

### Dashboard does not load

Check the following:

- Render service status
- Latest Render logs
- `requirements.txt` includes all dependencies
- Render start command is `gunicorn dashboard:server`
- `dashboard.py` includes `server = app.server`

### Charts do not display

Check the following:

- API key environment variables
- Terminal or Render logs for API errors
- Whether the selected metric exists for that source
- Whether the selected county has available data

### Render says `gunicorn: command not found`

Check the following:

- `gunicorn` is listed in `requirements.txt`
- Render is using the correct root directory
- Build cache has been cleared and redeployed

### Census or SAIPE returns errors

Check the following:

- Census API key is valid
- Invalid Census keys should be removed or replaced
- SAIPE metrics are limited to supported variables
- SAIPE data is delayed by release schedule

### WordPress display is too small

Try the following:

- Increase iframe height
- Use a full-width WordPress container
- Remove restrictive WordPress page layout settings

---

## Future Improvements

- Add caching to reduce repeated API calls
- Improve page load speed
- Add tooltips explaining each metric
- Add a real interactive county map
- Improve mobile responsiveness
- Add more county-level housing sources
- Add automatic scheduled data refresh logic
- Add user-friendly error messages for missing API data

---

## Summary

The Maryland Data Dashboard is a live, API-driven dashboard that allows users to explore Maryland statewide and county-level labor, housing, economic, and foreclosure indicators. The application is built with Dash and Plotly, deployed through Render, and embedded into WordPress for public access.