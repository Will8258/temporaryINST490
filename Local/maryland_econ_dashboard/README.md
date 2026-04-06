# Maryland Economics Data Dashboard

**Written by:** Donasyl Aho, Zainab Ahmadi, Max Eliker, Lily Gates, Joshua Kwan, and Ansh Rekhi  
_University of Maryland, College Park_

---

## Description
In collaboration with the National Center for Smart Growth Research and Education (NCSG) at the University of Maryland, College Park, this project produces CSV outputs and visualizations of economic indicators for Maryland counties and the state.  

We pull data from four sources:
* **FRED** — population, housing, GDP, and other economic indicators (`fred_api.py`).
* **BLS** — employment, unemployment rate, unemployment count, and labor force metrics (`bls_api.py`).
* **Socrata (Maryland Open Data)** — foreclosure filings data by county (`socrata_api.py`).
* **IPUMS NHGIS** — demographic time series data including population, race, sex, and age breakdowns (`ipums_api.py`).

The scripts handle:
* Retrieving series metadata (title, source, frequency, observation start/end)
* Fetching historical series data with retries and exponential backoff for rate limits
* Consistent snake_case file naming and organized folder structure

---

## Output
Running the API scripts creates CSV files organized by source:

* **FRED outputs**
    - County: `fred_csv_outputs/county_data/{county}/{county}_{series}.csv`
    - State: `fred_csv_outputs/state_data/{series}.csv`
* **BLS outputs**
    - Per-series county files: `bls_csv_outputs/county_data/separate/{county}_{metric}.csv`
    - Merged county files: `bls_csv_outputs/county_data/merged/{county}_all_metrics.csv`
* **Socrata outputs**
    - Per-county foreclosure metrics pivoted by type (e.g., NOI/NOF/FPR): `maryland_foreclosure_data/{COUNTY}.csv`
* **IPUMS outputs**
    - Demographic time series data for Maryland: `ipums_csv_outputs/md_demog/Maryland_{table_name}.csv`
    - Raw downloaded files: `ipums_csv_outputs/raw_zips/`

---

## Usage
1. Install dependencies:
```bash
pip install -r requirements.txt
```
2. Add API keys to `api_keys.yaml` in the repo root:
```yaml
fred_api: your_api_key_here
bls_api: your_api_key_here
ipums_api: your_api_key_here
```
3. Place `Indicators Series ID List.xlsx` in the repository root (same folder as the scripts). This file is required for `fred_api.py`, `bls_api.py`, `ipums_api.py`, and `generate_plotly_dash.py`.
4. Fetch FRED data:
```bash
python fred_api.py
```
5. Fetch BLS labor data:
```bash
python bls_api.py
```
6. Fetch Socrata foreclosure data (Maryland Open Data - no API key required):
```bash
python socrata_api.py
```
7. Fetch IPUMS NHGIS demographic data (requires "MD IPUMS NHGIS" sheet in the Excel file):
```bash
python ipums_api.py
```
8. (Optional) Explore dashboards/plots with `generate_plotly_dash.py` once FRED outputs exist. The script expects data in `fred_csv_outputs/state_data` and `fred_csv_outputs/county_data`.
9. (Optional) Generate choropleth maps of Maryland counties with `tile_map.py`. **Prerequisites:** Run `bls_api.py` first to generate merged county data. Navigate to `bls_csv_outputs/county_data/merged/` before running this script, as it looks for `*_all_metrics.csv` files in the current directory:
```bash
cd bls_csv_outputs/county_data/merged
python ../../../tile_map.py
```

---

## Output Structure
```
fred_csv_outputs/
├── county_data/
│   ├── montgomery/
│   │   ├── montgomery_resident_population.csv
│   │   ├── montgomery_unemployment_rate.csv
│   │   └── ...
│   └── prince_georges/
│       └── ...
└── state_data/
    ├── resident_population.csv
    ├── median_household_income.csv
    └── ...

bls_csv_outputs/
└── county_data/
    ├── separate/
    │   ├── allegany_employment.csv
    │   ├── allegany_unemployment_rate.csv
    │   └── ...
    └── merged/
        ├── allegany_all_metrics.csv
        └── ...

maryland_foreclosure_data/
├── ALLEGANY.csv
├── ANNE_ARUNDEL.csv
└── ...

ipums_csv_outputs/
├── md_demog/
│   ├── Maryland_Total_Population.csv
│   ├── Maryland_Sex.csv
│   ├── Maryland_Race_Short.csv
│   ├── Maryland_Race_Detailed_and_Age.csv
│   └── ...
└── raw_zips/
    └── nhgis####.zip
```

Each CSV contains:
* FRED/BLS: `date` – observation date; `value` – observed value for the series
* Socrata: `OBSERVATION DATE` plus foreclosure metrics columns (e.g., `NOI`, `NOF`, `FPR`) per county
* IPUMS: Time series demographic data with GISJOIN, YEAR, STATE, and metric columns (e.g., population counts, race breakdowns)

## Required Dependencies
This project requires the following Python libraries:
* `pandas` – for data manipulation
* `fredapi` – for accessing the FRED API
* `requests` – for API calls (BLS, IPUMS, Socrata, and map data)
* `pyyaml` – for loading API keys securely
* `openpyxl` – for reading .xlsx files
* `dash` and `plotly` – for dashboards and visualizations
* `prettytable` – for tabular CLI output


---

## Error Handling & Rate Limits
* If the FRED API returns a rate limit error (HTTP 429), the script waits and retries with exponential backoff (retry limits are configurable in the code).
* Any series that cannot be fetched is skipped and a warning is printed:
```
[WARN] Could not fetch series {series_id}. Skipping.
```
---

## Future Steps
* Add caching to prevent repeated API calls for unchanged series.
* Implement interactive dashboards or visualizations for county and state data.
* Include additional economic indicators as new FRED series become available.
* Add logging for API errors, retries, and skipped series.
* Handle partial data fetch resumption automatically if interrupted.
* Incorporate automated scheduling to update data regularly.
* Finish the alternative GitHub automation path in `Backup_Route/` (see `maryland_fred_github_automation.py` and `workflows/github_automation_refresh.yml`) to refresh data via continuous integration (CI) instead of manual runs.
