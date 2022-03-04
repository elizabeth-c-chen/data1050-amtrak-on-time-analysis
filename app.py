# IMPORTS

import os
import numpy as np
import pandas as pd
from textwrap import dedent
from datetime import date, datetime
from dash import Dash, dcc, html
from dash.dash_table import DataTable
import dash_bootstrap_components as dbc
import dash.exceptions
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
from utils import (
    connect_and_query,
    get_colors,
    get_days,
    get_precip_types,
    get_sort_from_direction,
)


#############################
# DASH SETUP
#############################
app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    update_title=None,
    external_stylesheets=[
        dbc.icons.BOOTSTRAP,
        "/assets/bootstrap-modified.css"
    ],
    meta_tags=[
        {
            'name': 'description',
            'content': 'Portfolio of Elizabeth C. Chen, Master\'s student studying data science at Brown University. Showcase for selected course projects.'
        },
        {
            'http-equiv': 'X-UA-Compatible',
            'content': 'IE=edge'
        },
        {
            'name': 'author',
            'content': 'Elizabeth Chen'
        },
        {
            'name': 'viewport',
            'content': 'width=device-width, initial-scale=1.0, shrink-to-fit=no'
        },
        {
            'name': 'theme-color',
            'content': '#001e69'
        }
    ]
)

server = app.server
app.title = "Elizabeth C. Chen"
app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content")
    ]
)

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

#############################
# INITIAL MAPBOX FIGURE
#############################
assert os.environ.get("MAPBOX_TOKEN") is not None, "empty token"
px.set_mapbox_access_token(os.environ.get("MAPBOX_TOKEN"))

geo_info_query = dedent(
    """
    SELECT
        station_code AS "STNCODE",
        amtrak_station_name as "STNNAME",
        longitude as "LON",
        latitude as "LAT",
        nb_mile AS "NB_MILE",
        sb_mile AS "SB_MILE"
    FROM
        station_info;
    """
)
geo_info = connect_and_query(geo_info_query).set_index("STNCODE")

geo_route_query = dedent(
    """
    SELECT
        longitude AS "Longitude",
        latitude AS "Latitude",
        CAST(path_group AS INTEGER) as "Group",
        connecting_path AS "Connecting Path",
        nb_station_group AS "NB Station Group",
        sb_station_group AS "SB Station Group"
    FROM
        regional_route;
    """
)
geo_route = connect_and_query(geo_route_query)


#############################
# STYLING AND MISC CONSTANTS
#############################
INPUT_STYLE = {"margin-right": "10px"}
FIGURE_STYLE = {"height": 560}
MAPBOX_STYLE = "mapbox://styles/elizabethchen/ckpwqldby4ta317nj9xfc1eeu"
CONTRAST_COLOR = "#001e69"
MARKER_STYLE = {"size": 6, "color": CONTRAST_COLOR}
PATH_STYLE = {"width": 3.5}
ZOOM_LEVEL = 5.9
FIGURE_LAYOUT_STYLE = {
    "paper_bgcolor": "white",
    "plot_bgcolor": "white",
    "margin": dict(t=20, l=25, b=20, r=0),
    "font": dict(size=10.5),
}
ALERT_CLASSNAME = "d-flex align-items-center"
CONTACT_EMAIL = "elizabeth_chen"
CONTACT_DOMAIN = "brown.edu"

#############################
# PATHNAME CONSTANTS
#############################
DATA1050_HOMEPAGE = "/data1050"
DATA1050_DETAILS = "/data1050-details"
DATA1050_ABOUT = "/data1050-about"
DATA1050_COMPARE = "/data1050-compare"


#############################
# REUSED ELEMENTS
#############################
data1050_disclaimer = html.P(
    "You are visiting the portfolio of Elizabeth C. Chen, Master's \
    student at Brown University. This webpage is not affiliated with \
    Amtrak in any way.",
    className="disclaimer"
)

portfolio_header = html.H4(
    html.A("Elizabeth C. Chen", href="/")
)

data1050_header = html.H5(
    html.A(
        "Amtrak Northeast Regional On-Time Performance Explorer: \
            DATA 1050 Final Project",
        href=DATA1050_HOMEPAGE,
    ),
    className="header-modified"
)

data1050_nav = dbc.Nav(
    [
        dbc.NavItem(
            dbc.NavLink(
                "About the Project",
                active="exact",
                href=DATA1050_ABOUT)
        ),
        dbc.NavItem(
            dbc.NavLink(
                "Project Technical Details", 
                active="exact",
                href=DATA1050_DETAILS
            )
        ),
        dbc.NavItem(
            dbc.NavLink(
                "On-Time Performance Visualizer",
                active="exact",
                href=DATA1050_HOMEPAGE
            )
        ),
        dbc.NavItem(
            dbc.NavLink(
                "On-Time Performance Comparison",
                active="exact",
                href=DATA1050_COMPARE,
            )
        ),
        dbc.DropdownMenu(
            [
                dbc.DropdownMenuItem(
                    "Visit Github Repository",
                    href="https://github.com/elizabeth-c-chen/data1050-amtrak-on-time-analysis",
                    id="button-link",
                ),
                dbc.DropdownMenuItem(
                    "Notebook: Data Retrieval/Database Loading",
                    href="https://nbviewer.jupyter.org/github/elizabeth-c-chen/data1050-amtrak-on-time-analysis/blob/master/EDA_ETL.ipynb",
                    id="button-link",
                ),
                dbc.DropdownMenuItem(
                    "Notebook: Setup Work for Route Visualization",
                    href="https://nbviewer.jupyter.org/github/elizabeth-c-chen/data1050-amtrak-on-time-analysis/blob/master/Determine_Station_Paths.ipynb",
                    id="button-link",
                ),
            ],
            label="View Source Code",
            id="nav-pills-special-dropdown",
            nav=True
        ),
    ],
    pills=True,
    horizontal="center",
    card=True,
    justified=True,
    className="nav-with-spacing"
)


#############################
# VISUALIZATION PAGE
#############################
default_query_df = pd.read_csv("./data/default_route_query.csv")
colors_dict, delays, counts, color_group_key = get_colors(geo_route, default_query_df)

route = px.line_mapbox(
    geo_route,
    lat=geo_route["Latitude"],
    lon=geo_route["Longitude"],
    line_group=geo_route["Connecting Path"],
    color=geo_route[color_group_key],
    color_discrete_map=colors_dict,
    hover_data={color_group_key: False, "Group": False},
    mapbox_style=MAPBOX_STYLE,
    zoom=ZOOM_LEVEL,
)

route.update_traces(line=PATH_STYLE)

route.add_trace(
    go.Scattermapbox(
        lat=geo_info["LAT"].round(decimals=5),
        lon=geo_info["LON"].round(decimals=5),
        name="Amtrak Stations",
        hoverinfo="text",
        customdata=np.stack([delays, counts], axis=-1),
        hovertext=np.stack([geo_info["STNNAME"], geo_info.index], axis=-1),
        hovertemplate="""
                    %{hovertext[0]} (%{hovertext[1]}) <br>
                    Avg. Delay: %{customdata[0]} mins
                    (<i>n</i> = %{customdata[1]})<extra></extra>""",
        mode="markers",
        marker=MARKER_STYLE,
        fill="none",
    )
)

route.update_layout(FIGURE_LAYOUT_STYLE)
route.update_traces(line=PATH_STYLE)
route.update_yaxes(automargin=True)

config = dict({"scrollZoom": False})

div_alert = html.Div(
    dbc.Alert(
        [
            "Showing results from default selection. (Hover over station \
                markers to view average delay information.)"
        ],
        color="info",
        className=ALERT_CLASSNAME,
        duration=7000,
    ),
    id="alert-msg",
)

controls = dbc.Card(
    [
        dbc.Row(
            dbc.Col(
                [
                    html.H6(html.B("Query Settings")),
                    html.P(
                        html.B(
                            "Changing the options below will modify the data\
                                selected for plotting.",
                            className="instructions"
                        )
                    ),
                ]
            )
        ),
        dbc.Row(
            dbc.Col(
                [
                    dbc.Label(
                        "Choose a direction for travel",
                        className="font-size-13"
                    ),
                    dcc.RadioItems(
                        id="direction-selector",
                        options=[
                            {"label": "Northbound", "value": "'Northbound'"},
                            {"label": "Southbound", "value": "'Southbound'"},
                        ],
                        value="'Southbound'",
                        inputStyle=INPUT_STYLE,
                        persistence=False,
                        className="options"
                    ),
                    dbc.Label(
                        "Select one or more days of the week to include",
                        className="font-size-13"
                    ),
                    dcc.Checklist(
                        id="days-of-week-checkboxes",
                        options=[
                            {"label": "Sunday", "value": 0},
                            {"label": "Monday", "value": 1},
                            {"label": "Tuesday", "value": 2},
                            {"label": "Wednesday", "value": 3},
                            {"label": "Thursday", "value": 4},
                            {"label": "Friday", "value": 5},
                            {"label": "Saturday", "value": 6},
                        ],
                        value=[0, 1, 2, 3, 4, 5, 6],
                        inputStyle=INPUT_STYLE,
                        className="options"
                    ),
                    dbc.Label(
                        "Select one or more precipitation conditions to include",
                        className="font-size-13"
                    ),
                    dcc.Checklist(
                        id="weather-type",
                        options=[
                            {"label": "No Precipitation", "value": "None"},
                            {"label": "Rain", "value": "Rain"},
                            {"label": "Snow", "value": "Snow"},
                        ],
                        value=["None", "Rain", "Snow"],
                        inputStyle=INPUT_STYLE,
                        className="options"
                    ),
                ],
            )
        ),
        dbc.Row(
            dbc.Col(
                html.Div(
                    [
                        dbc.Button(
                            html.P(
                                "Submit Query and Plot Results",
                                style={'font-size': '1vw', 'color': 'white'}
                            ),
                            id="send-query-button",
                            color="primary",
                            className="submit-btn"
                        ),
                    ],
                )
                
            )
        )
    ],
    id="controls",
    body=True
)

viz = dbc.Card(
    [
        html.Div(
            id="loading-wrapper",
            children= [
                dcc.Loading(
                    dcc.Graph(
                        id="geo-route",
                        config=config,
                        figure=route,
                        style=FIGURE_STYLE,
                        responsive=True,
                    ),
                    type="circle",
                    style={"background-color":"transparent"},  
                )
            ]
        )
    ],
    body=True
)


@app.callback(
    [Output("alert-msg", "children"), Output("geo-route", "figure")],
    [Input("send-query-button", "n_clicks")],
    [
        State("direction-selector", "value"),
        State("days-of-week-checkboxes", "value"),
        State("weather-type", "value"),
    ],
)
def generate_query(n_clicks, direction, days, weather_type):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate
    else:
        selected_days = get_days(days)
        weather_type = get_precip_types(weather_type)
        sort_stop_num = get_sort_from_direction(direction)
        query = dedent(
            f"""
            SELECT
                direction AS "Direction",
                station_code AS "Station",
                {sort_stop_num} AS "Stop Number",
                arrival_or_departure AS "Arrival or Departure",
                CAST(AVG(timedelta_from_sched) AS INTEGER) AS "Average Delay",
                COUNT(*) AS "Num Records"
            FROM
                stops_joined
            WHERE
                direction = {direction} AND
                sched_arr_dep_week_day IN {selected_days} AND
                precip_type IN {weather_type}
            GROUP BY station_code, direction, {sort_stop_num}, arrival_or_departure
            ORDER BY {sort_stop_num} ASC;
            """
        )
        try:
            query_df = connect_and_query(query, is_primary=True)
            assert query_df.shape[0] > 0
        except AssertionError:
            raise dash.exceptions.PreventUpdate
        colors, delays, counts, color_group_key = get_colors(geo_route, query_df)
        route = px.line_mapbox(
            geo_route,
            lat=geo_route["Latitude"],
            lon=geo_route["Longitude"],
            line_group=geo_route["Connecting Path"],
            color=geo_route[color_group_key],
            color_discrete_map=colors,
            hover_data={color_group_key: False, "Group": False},
            mapbox_style=MAPBOX_STYLE,
            zoom=ZOOM_LEVEL,
        )
        route.update_traces(line=PATH_STYLE)
        route.add_trace(
            go.Scattermapbox(
                lat=geo_info.LAT.round(decimals=5),
                lon=geo_info.LON.round(decimals=5),
                name="Amtrak Stations",
                hoverinfo="text",
                customdata=np.stack([delays, counts], axis=-1),
                hovertext=np.stack([geo_info["STNNAME"], geo_info.index], axis=-1),
                hovertemplate="""
                        %{hovertext[0]} (%{hovertext[1]}) <br>
                        Avg. Delay: %{customdata[0]} mins
                        (<i>n</i> = %{customdata[1]})<extra></extra>""",
                mode="markers",
                marker=MARKER_STYLE,
                fill="none",
            )
        )
        route.update_layout(FIGURE_LAYOUT_STYLE)
        route.update_yaxes(automargin=True)
        query_size = int(counts.sum())
        msg = dbc.Alert(
            [f"Queried {query_size} records. (Hover over station markers to view average delay information.)"],
            color="success",
            className=ALERT_CLASSNAME,
            duration=7000
        )

    return msg, route

#############################
# ENHANCEMENT PAGE
#############################

enhancement_view = dbc.Card(
    [
        dcc.Store(id="store", storage_type="session"),
        dbc.CardHeader(
            dbc.Tabs(
                [
                    dbc.Tab(label="View Trip Data", tab_id="trip"),
                    dbc.Tab(label="View Historical Averages", tab_id="history"),
                    dbc.Tab(label="View Database Query", tab_id="query"),
                ],
                id="card-tabs",
                active_tab="trip",
                persistence=False,
            )
        ),
        dbc.CardBody([html.H6("Change the settings on the left.")], id="card-content"),
    ],
    body=True,
    id="enhancement-view",
    className="auto-height"
)

enhancement_alert = html.Div(id="enhancement-alert")

specific_trip_controls = dbc.Card(
    [
        html.H6(html.B("Query Settings")),
        html.P(
            html.B(
                "Start by selecting date from the calendar below.",
                className="instructions"
            )
        ),
        dbc.Label(
            "1. Select a date to view past train trips.",
            className="label-down-margin"
        ),
        dcc.DatePickerSingle(
            id="single-trip-date-picker",
            display_format="MM-DD-YYYY",
            min_date_allowed=date(2011, 1, 1),
            max_date_allowed=date(2021, 8, 9),
            initial_visible_month=date(2021, 7, 1),
            placeholder="Use calendar picker or type a date",
            persistence=True,
            persistence_type="session",
        ),
        dbc.Label(
            "2. Select a train trip from the selected date.",
            id="step-2-label",
            className="label-up-down-margin"
        ),
        dcc.Dropdown(
            disabled=False,
            id="train-num-picker",
            searchable=True,
            clearable=False,
            placeholder="Choose from dropdown",
            persistence=True,
            persistence_type="session",
        ),
        dbc.Label(
            "3. Select a range of years to compare this trip with historical data.",
            id="step-3-label",
            className="label-up-down-margin"
        ),
        dcc.RangeSlider(
            min=2011,
            max=2021,
            value=[2011, 2019],
            marks={year: {"label": str(year)} for year in range(2011, 2022)},
            id="historical-range-slider",
            persistence=True,
            persistence_type="session"
        ),
        dbc.Button(
            "Submit Query and View Results",
            id="enhancement-send-query-button",
            color="primary",
            className="submit-btn",
            style={'font-size': '1vw', 'color': 'white'},
            disabled=True
        ),
    ],
    body=True,
)

@app.callback(
    Output("train-num-picker", "options"), [Input("single-trip-date-picker", "date")]
)
def show_step2(date_value):
    if date_value is not None:
        avail_trains_query = dedent(
            f"""
             SELECT DISTINCT
                CAST(train_num AS INTEGER) AS "Train Number"
             FROM
                stops_joined
             WHERE
                origin_date = '{date_value}'
             ORDER BY
                "Train Number" ASC;
            """
        )
        result = connect_and_query(avail_trains_query)
        if result.shape[0] > 0:
            train_num_options = [
                {"label": train_num, "value": train_num}
                for train_num in result["Train Number"]
            ]
            return train_num_options
        else:
            return []
            raise dash.exceptions.PreventUpdate
    else:
        raise dash.exceptions.PreventUpdate


@app.callback(
    Output("enhancement-send-query-button", "disabled"),
    [Input("train-num-picker", "value")],
)
def show_step3(train_num_selected):
    if train_num_selected is not None:
        return False
    else:
        return True


@app.callback(
    [
        Output("card-content", "children"),
        Output("store", "data"),
        Output("enhancement-alert", "children"),
    ],
    [
        Input("card-tabs", "active_tab"),
        Input("enhancement-send-query-button", "n_clicks"),
    ],
    [
        State("single-trip-date-picker", "date"),
        State("train-num-picker", "value"),
        State("historical-range-slider", "value"),
        State("store", "data"),
    ],
)
def enable_send_query(
    active_tab, n_clicks, selected_date, train_num, year_range, stored_views
):
    if active_tab:
        if n_clicks is not None:
            if train_num % 2 == 0:
                sort_stop_num = "nb_stop_num"
            else:
                sort_stop_num = "sb_stop_num"
            single_trip_query = dedent(
                f"""
                SELECT
                    station_code AS "Station",
                    {sort_stop_num} AS "Stop Number",
                    sched_arr_dep_time AS "Scheduled Time",
                    act_arr_dep_time AS "Actual Time",
                    arrival_or_departure AS "Arrival or Departure",
                    temperature AS "Temp (°F)",
                    precipitation AS "Precip (in)",
                    CAST(timedelta_from_sched AS integer) AS "Mins from Scheduled"
                FROM
                    stops_joined
                WHERE
                    origin_date = '{selected_date}' AND
                    train_num = '{train_num}'
                ORDER BY
                    {sort_stop_num} ASC, arrival_or_departure ASC;
                """
            )
            historical_query = dedent(
                f"""
                SELECT
                    station_code AS "Station",
                    {sort_stop_num} AS "Stop Number",
                    arrival_or_departure AS "Arrival or Departure",
                    ROUND(AVG(timedelta_from_sched), 1) AS "Avg. Mins from Scheduled",
                    ROUND(percentile_cont(0.25) WITHIN GROUP (ORDER BY timedelta_from_sched)) AS "Q1",
                    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY timedelta_from_sched)) AS "Median",
                    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY timedelta_from_sched)) AS "Q3",
                    COUNT(*) AS "Num Records Averaged"
                FROM
                    stops_joined
                WHERE
                    train_num = '{train_num}' AND
                    origin_year BETWEEN {year_range[0]} AND {year_range[1]}
                GROUP BY
                    station_code, {sort_stop_num}, arrival_or_departure
                ORDER BY
                    {sort_stop_num} ASC, arrival_or_departure ASC;
                """
            )
            single_trip_df = connect_and_query(single_trip_query, is_primary=True)
            if single_trip_df.shape[0] == 0:
                error_view = [
                    html.H6(
                        "An error occurred for this specific trip; please try another one!"
                    )
                ]
                alert = dbc.Alert(
                    [
                        f"""
                        Train {train_num} is not available for {selected_date}. (Hint: \
                            You moved so quickly that the database could not catch up with you in time!)
                        """
                    ],
                    color="warning",
                    duration=7000,
                    className=ALERT_CLASSNAME,
                )
                return error_view, None, alert
            historical_df = connect_and_query(historical_query, is_primary=True)
            fmt_date = datetime.strptime(selected_date, "%Y-%m-%d").strftime(
                "%b %d, %Y"
            )
            trip_view = [
                html.H6(f"Trip Data for Train {train_num} on {fmt_date}"),
                DataTable(
                    columns=[
                        {"name": col, "id": col} for col in single_trip_df.columns
                    ],
                    data=single_trip_df.to_dict("records"),
                    style_header={"backgroundColor": "rgb(183,224,248)"},
                    style_cell=dict(textAlign="left", fontSize="11.5px"),
                    sort_action="native",
                    id="data-table"
                ),
            ]
            if year_range[0] == year_range[1]:
                spec_title = f"Year {year_range[0]}"
            else:
                spec_title = f"Years {year_range[0]} to {year_range[1]}"
            historical_view = [
                html.H6(
                    f"Historical Data for Train {train_num} for " + spec_title,
                    className="card-title",
                ),
                DataTable(
                    columns=[{"name": col, "id": col} for col in historical_df.columns],
                    data=historical_df.to_dict("records"),
                    style_header={"backgroundColor": "rgb(183,224,248)"},
                    style_cell=dict(textAlign="left", fontSize="13px"),
                    sort_action="native",
                    id="data-table"
                ),
            ]
            query_view = [
                html.H6(
                    "This is the query that was used to retrieve historical data from the database."
                ),
                dcc.Markdown(
                    f"```\n{historical_query}\n```",
                    id="sql-query",
                ),
            ]
            stored_views = {
                "trip": trip_view,
                "history": historical_view,
                "query": query_view,
            }
            alert = dbc.Alert(
                [
                    f"Successfully queried database for Train {train_num} on {fmt_date}."
                ],
                color="success",
                duration=7000,
                className=ALERT_CLASSNAME,
            )
            return stored_views[active_tab], stored_views, alert
        elif n_clicks is None:
            if stored_views is not None:
                fmt_date = datetime.strptime(selected_date, "%Y-%m-%d").strftime(
                    "%b %d, %Y"
                )
                alert = dbc.Alert(
                    [
                        f"Showing previously queried data for Train\
                            {train_num} on {fmt_date}",
                    ],
                    color="info",
                    duration=7000,
                    className=ALERT_CLASSNAME,
                )
                return stored_views[active_tab], stored_views, alert
            else:
                alert = dbc.Alert(
                    [
                        "Hint: Change the settings above to view and compare data.",
                    ],
                    color="info",
                    duration=7000,
                    className=ALERT_CLASSNAME,
                )
                return [html.H6("Change the settings on the left!")], None, alert
        else:
            alert = dbc.Alert(
                [
                    "Hint: Change the settings above to view and compare data."
                ],
                color="info",
                duration=7000,
                className=ALERT_CLASSNAME,
            )
            return [html.H6("Change the settings on the left!")], None, alert
    else:
        alert = dbc.Alert(
            [
                "Hint: Change the settings above to view and compare data."
            ],
            color="info",
            duration=7000,
            className=ALERT_CLASSNAME,
        )
        return [html.H6("Change the settings on the left!")], None, alert


#############################
# ABOUT PROJECT PAGE
#############################
about_project = html.Div(
    [
        html.H4(html.B("About the Project")),
        html.P(
            [
                """
                This project was completed for the course DATA 1050: Data
                Engineering, as part of the Master's program in Data Science
                at Brown University. The goal of the assignment was to create
                a data science web application that showcases the dataset in an
                interesting and informative way, and allows for users to
                interact with the data and choose different aspects of the data
                being visualized. The detailed project requirements can be
                viewed
                """,
                html.A(
                    "here",
                    href="./assets/ProjectHandout.pdf",
                    className="underlined-text",
                ),
                "."
            ],
        ),
        html.Br(),
        html.P(
            """
            My vision for this project was to merge historical train delay data
            for stops along Amtrak's Northeast Regional route with the
            corresponding geographic weather information
            to create a way to understand the effects of weather conditions on
            the Northeast Regional's on-time performance. Having frequently
            traveled as a passenger on Amtrak's Northeast Regional and Acela
            trains since I started at Brown as a first-year student, I was
            inspired to explore the performance of trains along the route when
            compared with the associated historical weather data. I chose to
            focus on the route between Boston, Massachusetts and Washington,
            D.C. and most intermediate stations. The data collected spans from
            2011 to August 2021.
            """
        ),
        html.Br(),
        html.P(
            """
            I hope to extend this project with improvements and enhancements
            in the future. Some ideas for additions include:
            """
        ),
        html.Ul(
            [
                html.Li(
                    """
                    Trying various machine learning techniques and time series
                    analysis on the dataset with the goal of predicting whether
                    or not a train will be delayed by more than a certain
                    number of minutes
                    """,
                ),
                html.Li(
                    """
                    A trip simulator where users can set specific trip
                    parameters (origin and destination station, time
                    of day and weekday, weather conditions, etc.) and
                    receive an estimate for likelihood of delays.
                    """
                ),
                html.Li(
                    """
                    Ability for any user to choose variables from the dataset
                    and create graphs and figures from the selected data
                    """
                ),
                html.Li(
                    """
                    Adding more data sources and variables such as information
                    from the NOAA Severe Weather Data Inventory, national
                    holidays and other major events, and more
                    """
                ),
            ],
            className="bullet-offset"
        ),
        html.H5(html.B("Thank you!")),
        html.P(
            [
                """
                This project would not be possible without the diligent joint
                effort by """,
                html.A(
                    "Chris Juckins",
                    href="https://juckins.net/index.php",
                    className="underlined-text",
                ),
                " and ",
                html.A(
                    "John Bobinyec",
                    href="http://dixielandsoftware.net/Amtrak/status/StatusMaps/",
                    className="underlined-text",
                ),
                " to collect and preserve Amtrak's on-time performance records.",
                """
                I am extremely grateful for Chris' enthusiastic support of
                my project and for allowing me to use data from his website.
                The timetable archives were also an invaluable source of
                information for me while working on this project. The train
                data is sourced from
                """,
                html.A(
                    "Amtrak Status Maps Archive Database (ASMAD)",
                    href="https://juckins.net/amtrak_status/archive/html/home.php",
                    className="underlined-text",
                ),
                " on Chris Juckins' website. The weather data is sourced from ",
                html.A(
                    "Visual Crossing's Weather API",
                    href="https://www.visualcrossing.com",
                    className="underlined-text",
                ),
                """
                . The geospatial data used to create the route visualization
                and stations along the route was retrieved from the U.S.
                Department of Transportation's
                """,
                html.A(
                    "Open Data Catalog",
                    href="https://data-usdot.opendata.arcgis.com",
                    className="underlined-text",
                ),
                ".",
            ]
        ),
    ]
)


#############################
# TECHNICAL DETAILS PAGE
#############################
details = html.Div(
    [
        html.H4(html.B("Technical Details")),
        html.H5("Datasets Used"),
        html.P(
            """
            The datasets used in this project include Amtrak train arrival and
            departure data, weather data, and geographic information files
            containing the route and station coordinates for the Northeast
            Regional route.
            """
        ),
        html.H5("Project Architecture"),
        html.P(
            """
            The diagram below shows an overview of the project architecture.
            This project was written entirely in Python and uses the Dash
            and Plotly libraries to create the interactive features.
            The application uses a Postgres database and is hosted on Heroku.
            Previously, new data corresponding to trains and weather
            observations from the prior day was loaded into the database
            automatically each day at noon. This automatic retrieval process
            is no longer active, but may be restored if I extend the project
            in the future.
            """
        ),
        html.Img(
            src="./assets/Project_Architecture.png",
            title="Project Architecture",
            className="image-fit"
        ),
        html.H5("Data Acquisition and Database Schema"),
        html.P(
            """
            The train and weather data are retrieved using GET requests to the
            Amtrak Status Maps Archive Database (ASMAD) and Visual Crossing API,
            respectively. All available past data was initially loaded into the
            database, and each day after ASMAD updates, the application submits
            a query for the previous day's train and weather data and processes
            this data, then loads it into the database. Initially the weather
            and train data are in separate tables, but the data is later joined
            and inserted into another table which stores the joined data. The
            data which is shown in the Visualization upon loading is saved as
            a file rather than loaded via a query to the database to reduce
            the initial loading time.
            """
        ),
        html.P(
            """
            The database table structure is shown in the diagram below. An index
            on the train number field speeds up query time significantly in the
            On-Time Performance Analysis section, and indexes on precipitation
            type and day of week also show some performance improvements for
            the Visualization queries.
            """
        ),
        html.Img(
            src="./assets/Database_Schema.png",
            title="Database Schema",
            className="image-fit"
        ),
        html.H5("Source Code"),
        html.P(
            """
            The final versions of code I wrote for this project, including the
            data retrieval functions and Jupyter Notebooks of my work, can be
            viewed by visiting links in the "View Source Code" dropdown menu
            in the navigation bar above.
            """
        ),
    ]
)


#############################
# HOMEPAGE
#############################
profile_img = html.Img(
    src="./assets/profile.png",
    id="profile-round",
    alt="waving-student",
)

home_nav = dbc.Nav(
    [
        html.H3(html.A(html.B("Elizabeth C. Chen"), href="/")),
        profile_img,
        dbc.NavItem(
            html.H4(
                html.B("Project Links", style={'font-size': '30px', 'padding-bottom': '-1rem'})
            ),
        ),
        dbc.NavItem(
            dbc.NavLink(
                [
                    "Amtrak On-Time Performance Explorer Interactive App",
                    html.I(className="bi-arrow-right-circle arrow-icon")
                ],
                href=DATA1050_HOMEPAGE,
                class_name="btn mr-md-2 mb-md-0 mb-2 btn-goto"
            )
        ),
        dbc.NavItem(
            dbc.NavLink(
                [
                    "Machine Learning Applied to Automated Theorem Proving",
                    html.I(className="bi-arrow-right-circle arrow-icon")
                ],
                href="https://github.com/elizabeth-c-chen/data1030-ML-theorem-proving",
                class_name="btn mr-md-2 mb-md-0 mb-2 btn-goto"
            )
        ),
    ],
    vertical=True,
    pills=True
)

bio_text = html.Div(
    [
        html.P(
            """
            Hello, thanks for stopping by! My name is Elizabeth and I'm a
            graduate student at Brown University working towards
            a Master's degree in Data Science.
            """,
            className="font-size-par"
        ),
        html.Br(),
        html.P(
            [
                """
                This site hosts the independent projects I have completed
                during the program. For source code and more, please visit my
                """,
                html.A(
                    href="https://github.com/elizabeth-c-chen/",
                    children="GitHub profile",
                    className="underlined-text"),
                "."
            ],
            className="font-size-par"
        )
    ],
    className="margin-top-20"
)

index_page_layout = dbc.Container(
    [
        dbc.Row(html.Br()),
        dbc.Row(
            [
                dbc.Col(home_nav, md=5, className="col-md", ),
                dbc.Col(bio_text, md=7, className="col-lg"),
            ]
        ),
    ]
)

data1050_app_about_layout = dbc.Container(
    [
        portfolio_header,
        data1050_header,
        data1050_nav,
        dbc.Row(
            [
                dbc.Col(about_project, md=10, lg=9)
            ],
            class_name="text-page-content"
        ),
        dbc.Row(data1050_disclaimer),
    ],
    fluid=True,
)

data1050_app_details_layout = dbc.Container(
    [
        portfolio_header,
        data1050_header,
        data1050_nav,
        dbc.Row(
            [
                dbc.Col(details, md=10, lg=9)
            ],
            class_name="text-page-content"
        ),
        dbc.Row(data1050_disclaimer),
    ],
    fluid=True,
)

data1050_app_viz_layout = dbc.Container(
    [
        portfolio_header,
        data1050_header,
        data1050_nav,
        dbc.Row(
            [
                dbc.Col([controls, div_alert], md=4, lg=3.25),
                dbc.Col(viz, md=8, lg=8.75)
            ]
        ),
        dbc.Row(data1050_disclaimer),
    ],
    fluid=True,
)


data1050_app_enhancement_layout = dbc.Container(
    [
        portfolio_header,
        data1050_header,
        data1050_nav,
        dbc.Row(
            [
                dbc.Col([specific_trip_controls, enhancement_alert], md=5, lg=4),
                dbc.Col(enhancement_view, md=7, lg=8),
            ]
        ),
        dbc.Row(data1050_disclaimer),
    ],
    fluid=True,
)


error_page_layout = dbc.Container(
    [
        portfolio_header,
        dbc.Card(
            [
                html.H5(
                    "Uh oh! You have reached a page that doesn't exist.",
                ),
                html.H2(
                    html.I(className="bi bi-emoji-frown"),
                    style={
                        "margin-top": "5rem !important",
                        "margin-bottom": "3rem !important"
                    }
                ),
                html.H6(
                    html.A(
                        "Click here to return to the homepage.",
                        href="/",
                        className="underlined-text"
                    )
                )
            ],
            className="error-404-card"
        )
    ],
    className="centered-div",
    fluid=True
)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname):
    if pathname == "/":
        return index_page_layout
    elif pathname == DATA1050_ABOUT:
        return data1050_app_about_layout
    elif pathname == DATA1050_DETAILS:
        return data1050_app_details_layout
    elif pathname == DATA1050_HOMEPAGE:
        return data1050_app_viz_layout
    elif pathname == DATA1050_COMPARE:
        return data1050_app_enhancement_layout
    else:
        return error_page_layout


if __name__ == "__main__":
    app.run_server(port=8050, debug=False)
