import os
import time
from textwrap import dedent

import pandas as pd
import psycopg2

import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State

import plotly.express as px
import plotly.graph_objects as go


# Helper Functions
def connect_and_query(query):
    conn = psycopg2.connect("dbname='amtrakproject' user='appuser' \
    password={}".format(os.environ.get('DB_PASS')))
    query_data = pd.read_sql(query, conn)
    conn.close()
    return query_data


def get_days(days_selected):
    output = '('
    day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday',
                 'Thursday', 'Friday', 'Saturday']
    for i in range(len(day_names)):
        if i in days_selected:
            output += '\'' + day_names[i] + '\'' + ', '
    output = output[0:-2] + ')'
    return output


# Dash setup
app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                suppress_callback_exceptions=True)
server = app.server
app.title = 'DATA 1050 Project'

# Mapbox setup
assert os.environ.get('MAPBOX_TOKEN') is not None, 'empty mapbox token!'
px.set_mapbox_access_token(os.environ.get('MAPBOX_TOKEN'))

# Database setup
assert os.environ.get('DB_PASS') is not None, 'empty database password!'

# Load route and stations info through queries to database
geo_info_query = dedent(
    """
    SELECT
        station_code AS STNCODE,
        station_name as STNNAME,
        longitude as LON,
        latitude as LAT
    FROM
        station_info
    """
)
geo_info = connect_and_query(geo_info_query)
geo_route = pd.read_csv('./data/facts/NE_regional_lonlat.csv')  # Replace w DB query later

# Info for map -- change later
amtrak_stations = list(geo_info['STNCODE'])
location_names = list(geo_info['STNNAME'])
map_style = 'outdoors'

# Route Visualization with Stand-in Color Coded Groups
route = px.line_mapbox(geo_route,
                       lat=geo_route['Latitude'],
                       lon=geo_route['Longitude'],
                       line_group=geo_route['Group'],
                       color=geo_route['Connecting Path'],
                       color_discrete_sequence=px.colors.qualitative.T10,
                       hover_data={'Group': False},
                       mapbox_style=map_style,
                       zoom=6)
route.update_traces(line=dict(width=3))

route.add_trace(go.Scattermapbox(lat=geo_info['LAT'].round(decimals=5),
                                 lon=geo_info['LON'].round(decimals=5),
                                 name='Amtrak Stations',
                                 hoverinfo='text',
                                 customdata=geo_info['STNCODE'],
                                 hovertext=geo_info['STNNAME'],
                                 hovertemplate="%{hovertext} (%{customdata})<extra></extra>",
                                 mode='markers',
                                 marker={'size': 6, 'color': 'Navy'},
                                 fill='none'
                                 )
                )
route.update_layout(
    dict(paper_bgcolor="white", plot_bgcolor="white",
         margin=dict(t=35, l=80, b=0, r=0), height=500))
route.update_yaxes(automargin=True)

config = dict({'scrollZoom': False})

# Components of homepage layout

div_alert = dbc.Spinner(html.Div(id="alert-msg"))

query_container = dbc.Card(
        [
            html.H5("Auto-generated PostgreSQL Query", className='card-title'),
            dcc.Markdown(id='auto-sql-query')
        ],
        body=True
)

controls = dbc.Card(
    [
        dbc.FormGroup(
            [
                dbc.Label('Choose a direction for travel'),
                dcc.RadioItems(
                    id='direction-selector',
                    options=[{'label': 'Northbound', 'value': 'Northbound'},
                             {'label': 'Southbound', 'value': 'Southbound'}],
                    value='Southbound'
                ),
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Select one or more days of the week to include'),
                dcc.Checklist(
                    id='days-of-week-checkboxes',
                    options=[
                            {'label': 'Sunday', 'value': 0},
                            {'label': 'Monday', 'value': 1},
                            {'label': 'Tuesday', 'value': 2},
                            {'label': 'Wednesday', 'value': 2},
                            {'label': 'Thursday', 'value': 4},
                            {'label': 'Friday', 'value': 5},
                            {'label': 'Saturday', 'value': 6}
                    ],
                    value=[0, 1, 2, 3, 4, 5, 6],
                )
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Select precipitation conditions to include'),
                dcc.RadioItems(
                    id='weather-conditions-selector',
                    options=[
                            {'label': 'Rain', 'value': '(\'Rain\')'},
                            {'label': 'Snow', 'value': '(\'Snow\')'},
                            {'label': 'Rain + Snow', 'value': '(\'Rain\', \'Snow\')'},
                            {'label': 'No Precipitation', 'value': '(\'No Precipitation\')'}
                    ],
                    value=['Snow']
                )
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Enter a range of allowed delays in minutes (max = 600)'),
                dcc.Input(id='min-delay', placeholder='0', type='number', min=0, max=599),
                dcc.Input(id='max-delay', placeholder='30', type='number', min=0, max=600)
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Allow data with known Service Disruptions in query?'),
                dcc.RadioItems(
                    id='allow-sd-choice',
                    options=[
                        {'label': 'Yes', 'value': '1'},
                        {'label': 'No', 'value': '0'}],
                    value='False')
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Allow data with known Cancellations in query?'),
                dcc.RadioItems(
                    id='allow-cancel-choice',
                    options=[
                        {'label': 'Yes', 'value': '1'},
                        {'label': 'No', 'value': '0'}
                    ],
                    value='False')
            ]
        ),
        dbc.Button("Query database and plot results", color="primary", id='send-query-button')
    ],
    body=True,
)

app.layout = dbc.Container(
    [
        html.H1("DATA 1050 Final Project"),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(controls, lg=5),
                dbc.Col(
                    dcc.Graph(
                        id='geo-route',
                        config=config,
                        figure=route,
                        style={"height": "800px"}),
                    lg=7),
            ],
            no_gutters=True
        ),
        dbc.Row(
            [
                dbc.Col(query_container, lg=5)
            ]
        )
    ],
    fluid=True,
)


@app.callback(
    [
        Output("auto-sql-query", "children"),
        Output("alert-msg", "children"),
        Output("geo-route", "figure")
    ],
    [
        Input("send-query-button", 'n_clicks')
    ],
    [
        State('direction-selector', 'value'),
        State('days-of-week-checkboxes', 'value'),
        State('weather-conditions-selector', 'value'),
        State('min-delay', 'value'),
        State('max-delay', 'value'),
        State('allow-sd-choice', 'value'),
        State('allow-cancel-choice', 'value')
    ]
)
def generate_query(n_clicks, dir, days, weather, min_d, max_d, sd, cancel):
    t0 = time.time()
    selected_days = get_days(days)
    query = dedent(
        f"""
        SELECT
            d.station_code AS "Station",
            ROUND(AVG(d.depart_diff), 2) AS "Average Delay",
            COUNT(*) AS "Num Records"
        FROM =
            departures d
            INNER JOIN (
            SELECT
                conditions,
                date_time,
                location,
                si.station_code AS station_code
            FROM
                weather_hourly wh
                INNER JOIN (
                    SELECT
                        station_code,
                        weather_loc
                    FROM
                        station_info
                ) si ON wh.location = si.weather_loc
            WHERE
                wh.conditions IN {weather}
            ) wh ON DATE_TRUNC('hour', d.full_sched_dep_datetime) = wh.date_time AND
              wh.station_code = d.station_code
        WHERE
            d.direction = {dir} AND
            d.origin_week_day IN {selected_days} AND
            d.depart_diff BETWEEN {min_d} AND {max_d} AND
            d.service_disruption = {sd} AND
            d.cancellations = {cancel}
        GROUP BY d.station_code;
        """
    )
    query_df = connect_and_query(query)

    query_size = query_df['Num Records'].sum()
    elapsed = time.time() - t0
    alert_msg = f"Queried {query_size} records. Total time: {elapsed:.2f}s."
    alert = dbc.Alert(alert_msg, color="success", dismissable=True)
    return alert, query_df


if __name__ == '__main__':
    app.run_server(debug=True)
