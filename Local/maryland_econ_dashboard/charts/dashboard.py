from dash import Dash, html, dcc, Input, Output, ctx, ALL, no_update
import plotly.graph_objects as go
import requests
import pandas as pd

app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server

FRED_API_KEY = "9e5d944924918a26660adf4a492e447e"
FORECLOSURE_API_URL = "https://opendata.maryland.gov/resource/w3bc-8mnv.json"

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

CATEGORIES = {
    "labor": {"title": "MARYLAND LABOR STATISTICS:", "buttons": list(LABOR_SERIES.keys())},
    "housing": {"title": "MARYLAND HOUSING STATISTICS:", "buttons": list(HOUSING_SERIES.keys())},
    "economic": {"title": "MARYLAND ECONOMIC STATISTICS:", "buttons": list(ECONOMIC_SERIES.keys())},
}


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


def fetch_foreclosures():
    params = {"$limit": 50000, "$order": "date ASC"}
    res = requests.get(FORECLOSURE_API_URL, params=params, timeout=30)
    res.raise_for_status()

    df = pd.DataFrame(res.json())
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    ignored = {"date", "type", ":id", ":created_at", ":updated_at", ":version"}
    county_cols = [c for c in df.columns if c not in ignored]

    for c in county_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["statewide_total"] = df[county_cols].sum(axis=1)

    return df[["date", "type", "statewide_total"]].sort_values("date")


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
    fig.update_layout(title=message, template="plotly_white", height=500)
    return fig


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


app.layout = html.Div([
    dcc.Store(id="selected-category", data=None),
    dcc.Store(id="selected-subcategory", data=None),

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


@app.callback(
    Output("page-content", "children"),
    Input("selected-category", "data"),
    Input("selected-subcategory", "data")
)
def render_page(category, subcategory):
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
                    category_button("Maryland Economic Statistics", "economic")
                ], style={"display": "flex", "justifyContent": "center"})
            ], style={
                "border": "2px solid #2f568d",
                "boxShadow": "0 4px 10px rgba(0,0,0,0.25)",
                "padding": "55px",
                "maxWidth": "850px",
                "margin": "0 auto"
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
                category_button("Maryland Economic Statistics", "economic", selected=(category == "economic"))
            ], style={
                "display": "flex",
                "justifyContent": "center",
                "marginBottom": "25px"
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


@app.callback(
    Output("selected-category", "data"),
    Output("selected-subcategory", "data", allow_duplicate=True),
    Input({"type": "category-btn", "category": ALL}, "n_clicks"),
    Input("back-btn", "n_clicks"),
    prevent_initial_call=True
)
def update_category(category_clicks, back_clicks):
    triggered = ctx.triggered_id

    if triggered == "back-btn":
        if back_clicks and back_clicks > 0:
            return None, None
        return no_update, no_update

    if isinstance(triggered, dict):
        if not category_clicks or max(category_clicks) == 0:
            return no_update, no_update

        return triggered["category"], None

    return no_update, no_update


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