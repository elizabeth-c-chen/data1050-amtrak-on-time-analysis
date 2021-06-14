import os
from textwrap import dedent
import time 

import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State

import plotly.express as px
import plotly.graph_objects as go

from utils import *

# Dash setup
app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                suppress_callback_exceptions=True)
server = app.server
app.title = 'DATA 1050 Project'

# Mapbox setup
assert os.environ.get('MAPBOX_TOKEN') is not None, 'empty mapbox token!'
px.set_mapbox_access_token(os.environ.get('MAPBOX_TOKEN'))

# Load route and stations info through queries to database
geo_info_query = dedent(
    """
    SELECT
        station_code AS "STNCODE",
        amtrak_station_name as "STNNAME",
        longitude as "LON",
        latitude as "LAT"
    FROM
        station_info;
    """
)
geo_info = connect_and_query(geo_info_query)

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

default_query = dedent(
            """
            SELECT
                t.direction AS "Direction",
                t.station_code AS "Station",
                t.sb_mile,
                t.arrival_or_departure AS "Arrival or Departure",
                CAST(AVG(t.timedelta_from_sched) AS INTEGER) AS "Average Delay",
                COUNT(*) AS "Num Records"
            FROM
                stops_joined t
            WHERE
                t.direction = 'Southbound' AND
                t.sched_arr_dep_week_day IN
                    ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday')
            GROUP BY t.station_code, t.direction, t.sb_mile, t.arrival_or_departure
            ORDER BY t.sb_mile ASC;
            """
)
default_query_df = connect_and_query(default_query)
colors_dict, delays, color_group_key = get_colors(geo_route, default_query_df)

# Info for map
amtrak_stations = list(geo_info['STNCODE'])
location_names = list(geo_info['STNNAME'])
#map_style = 'mapbox://styles/elizabethchen/ckpwoy47551i018mntxuxsge1'
#contrast_color = '#EDEDED'
#map_style = 'mapbox://styles/elizabethchen/ckpwpfj0v1xak17mwoyasccsm'
#map_style = 'light'
#contrast_color = 'black'

map_style = 'mapbox://styles/elizabethchen/ckpwqldby4ta317nj9xfc1eeu'
contrast_color = 'navy'
# Route Visualization with Stand-in Color Coded Groups
route = px.line_mapbox(geo_route,
                       lat=geo_route['Latitude'],
                       lon=geo_route['Longitude'],
                       line_group=geo_route['Connecting Path'],
                       color=geo_route[color_group_key],
                       color_discrete_map=colors_dict,
                       hover_data={color_group_key: False, 'Group': False},
                       mapbox_style=map_style,
                       zoom=6.15)
#route.update_mapboxes(style=)
route.update_traces(line=dict(width=3))

route.add_trace(go.Scattermapbox(lat=geo_info['LAT'].round(decimals=5),
                                 lon=geo_info['LON'].round(decimals=5),
                                 name='Amtrak Stations',
                                 hoverinfo='text',
                                 customdata=delays,
                                 hovertext=geo_info['STNNAME'],
                                 hovertemplate="%{hovertext} (Avg. Delay: %{customdata} mins)<extra></extra>",
                                 mode='markers',
                                 marker={'size': 6, 'color': contrast_color},
                                 fill='none'
                                 )
                )
route.update_layout(
    dict(paper_bgcolor="white", plot_bgcolor="white",
         margin=dict(t=35, l=80, b=0, r=0)))

route.update_yaxes(automargin=True)

config = dict({'scrollZoom': False})

# Components of homepage layout

div_alert = html.Div(id="alert-msg")

controls = dbc.Card(
    [
        dbc.FormGroup(
            [
                dbc.Label('Choose a direction for travel', style={'font-size': 15}),
                dcc.RadioItems(
                    id='direction-selector',
                    options=[{'label': 'Northbound', 'value': "\'Northbound\'"},
                             {'label': 'Southbound', 'value': "\'Southbound\'"}],
                    value="\'Southbound\'",
                    inputStyle={"margin-right": "10px"},
                    style={'font-size': 14, 'padding-left': '5%'},
                    persistence=True
                ),
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Select one or more days of the week to include', style={'font-size': 15}),
                dcc.Checklist(
                    id='days-of-week-checkboxes',
                    options=[
                            {'label': 'Sunday', 'value': 0},
                            {'label': 'Monday', 'value': 1},
                            {'label': 'Tuesday', 'value': 2},
                            {'label': 'Wednesday', 'value': 3},
                            {'label': 'Thursday', 'value': 4},
                            {'label': 'Friday', 'value': 5},
                            {'label': 'Saturday', 'value': 6}
                    ],
                    value=[0, 1, 2, 3, 4, 5, 6],
                    inputStyle={"margin-right": "10px"},
                    style={'font-size': 14, 'padding-left': '5%'},
                    persistence=True
                )
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Allow data with known Service Disruptions?', style={'font-size': 15}),
                dcc.RadioItems(
                    id='sd-selector',
                    options=[
                        {'label': 'Yes', 'value': "\'0\' OR t.service_disruption = \'1\'"},
                        {'label': 'No', 'value': "\'0\'"}],
                    value="\'0\'",
                    inputStyle={"margin-right": "10px"},
                    style={'font-size': 14, 'padding-left': '5%'},
                    persistence=True
                )
            ]
        ),

        dbc.FormGroup(
            [
                dbc.Label('Allow data with known Cancellations?', style={'font-size': 15}),
                dcc.RadioItems(
                    id='cancellation-selector',
                    options=[
                        {'label': 'Yes', 'value': "\'0\' OR t.cancellations = \'1\'"},
                        {'label': 'No', 'value': "\'0\'"}
                    ],
                    value="\'0\'",
                    inputStyle={"margin-right": "10px"},
                    style={'font-size': 14, 'padding-left': '5%'},
                    persistence=True
                )
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Select one of the following options to further filter the data', style={'font-size': 15}),
                dcc.RadioItems(
                    id='filter-type-selector',
                    options=[{'label': 'Extreme Temperatures', 'value': "extreme-temp"},
                             {'label': 'Season', 'value': "seasons"},
                             {'label': 'Cloud Coverage', 'value': "cloud-cover"},
                             {'label': 'Precipitation by Type', 'value': "precip-by-type"},
                             {'label': 'Precipitation by Amount', 'value': "precip-by-amount"}
                             ],
                    value="extreme-temp",
                    inputStyle={"margin-right": "10px"},
                    style={'font-size': 14, 'padding-left': '5%'},
                    persistence=True
                )
            ]
        ),
        dbc.Button(
            'View Filtering Options',
            color='primary',
            id='set-filter-mode',
            style={'font-size': 15}
        ),
        dbc.FormGroup(id='chosen-option-formgroup'),
        dbc.Button(
            "Submit Query and Plot Results",
            id="send-query-button",        
            color="primary",
            style={'font-size': 15, 'margin-top': '2.5%'},
            disabled=True
        )
    ],
    id="controls",
    body=True
)

temp_checkboxes = [
    dbc.Label('Select temperature conditions', style={'font-size': 15, 'padding-top': '2.5%'}),
    dcc.Checklist(
        id='chosen-option',
        options=[
                {'label': 'Extreme Cold (temperature < 32\u00B0)', 'value': 'cold'},
                {'label': 'Extreme Heat (temperature > 90\u00B0)', 'value': 'hot'},
                {'label': 'Moderate Range (32\u00B0 ≤ temperature ≤90\u00B0) ', 'value': 'between'}
        ],
        value=['hot'],
        inputStyle={"margin-right": "10px"},
        style={'font-size': 14, 'padding-left': '5%', 'margin-bottom': '-15px'},
        persistence=True
    )
]

        
cloud_cover_checkboxes = [
    dbc.Label('Select one or more levels of cloudiness', style={'font-size': 15, 'padding-top': '2.5%'}),
    dcc.Checklist(
        id='chosen-option',
        options=[
            {'label': 'Clear', 'value': 'clear'},
            {'label': 'Mostly Sunny to Partly Cloudy', 'value': 'partly cloudy'},
            {'label': 'Partly Sunny to Mostly Cloudy', 'value': 'mostly cloudy'},
            {'label': 'Overcast', 'value': 'overcast'}
        ],
        value=['clear'],
        inputStyle={"margin-right": "10px"},
        style={'font-size': 14, 'padding-left': '5%', 'margin-bottom': '-15px'},
        persistence=True
    )
]

precip_by_type = [
    dbc.Label('Select one or more precipitation conditions to include', style={'font-size': 15, 'padding-top': '2.5%'}),
    dcc.Checklist(
        id='chosen-option',
        options=[
                {'label': 'No Precipitation', 'value': 'none'},
                {'label': 'Rain', 'value': 'rain'},
                {'label': 'Snow', 'value': 'snow'},
                {'label': 'Other', 'value': 'other'}
        ],
        value=['rain', 'snow'],
        inputStyle={"margin-right": "10px"},
        style={'font-size': 14, 'padding-left': '5%', 'margin-bottom': '-15px'},
        persistence=True
    )
]

precip_by_amount = [
    dbc.Label('Select a range of precipitation levels (rain, snow, and other precipitation included)', style={'font-size': 15, 'padding-top': '2.5%'}),
    dcc.RangeSlider(
        id='chosen-option',
        min=0,
        max=3,
        step=None,
        marks={
            0: 'None',
            1: 'Light',
            2: 'Moderate',
            3: 'Heavy'
        },
        value=[0,1],
        persistence=True
    )
]

seasons = [
    dbc.Label('Select one or more seasons to include', style={'font-size': 15, 'padding-top': '2.5%'}),
    dcc.Checklist(
        id='chosen-option',
        options=[
                {'label': 'Fall', 'value': 3},
                {'label': 'Winter', 'value': 0},
                {'label': 'Spring', 'value': 1},
                {'label': 'Summer', 'value': 2}
            ],
        value=[0,1,2,3],
        inputStyle={"margin-right": "10px"},
        style={'font-size': 14, 'padding-left': '5%', 'margin-bottom': '-15px'},
        persistence=True
    )
]

viz = dbc.Card(
    [
        dcc.Graph(
            id='geo-route',
            config=config,
            figure=route,
            style=dict(height=550, width=1000))
    ],
    body=True
)

app.layout = dbc.Container(
    [
        html.H2("Amtrak Northeast Regional On-Time Performance Explorer"),
        html.H5("A DATA 1050 Final Project by Elizabeth C. Chen", style={'padding-top': '-10px', 'padding-bottom': '-10px'}),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col([controls, div_alert], md=4, lg=3.25),
                dbc.Col(viz, md=8, lg=8.75)
            ],
            no_gutters=False
        ),
        dbc.Row(
            [
                html.P(
                    children="You are visiting the portfolio of Elizabeth C. Chen, Master's \
                        student at Brown University. This webpage is not affiliated with \
                        Amtrak in any way.",
                    style={'font-size': 12, 'display': 'block', 'padding-top': '3%', 'margin-left': 'auto', 'margin-right': 'auto'}
                )
            ]
        )
    ],
    fluid=True
)

submit_callback_states_list = get_submit_callback_states_list()

@app.callback(
    [
        Output("chosen-option-formgroup", "children"), 
        Output("send-query-button", "disabled")
    ],
    [
        Input("set-filter-mode", "n_clicks")
    ],
    [
        State("filter-type-selector", "value")
    ]
)
def update_options(n_clicks, option_val):
    if n_clicks is None:
        submit_callback_states_list = set_submit_callback_states_list(n_clicks)
        raise dash.exceptions.PreventUpdate
    else: 
        submit_callback_states_list = set_submit_callback_states_list(n_clicks)
        if option_val == "extreme-temp":
            choice = temp_checkboxes
        elif option_val == "seasons":
            choice = seasons
        elif option_val == "cloud-cover":
            choice = cloud_cover_checkboxes
        elif option_val == "precip-by-type":
            choice = precip_by_type
        elif option_val == "precip-by-amount":
            choice = precip_by_amount
        return choice, False

default_options = ['snow', 'rain', 'none', 'other']

@app.callback(
    [
        Output("alert-msg", "children"),
        Output("geo-route", "figure")
    ],
    [
        Input("send-query-button", 'n_clicks')
    ],
    submit_callback_states_list
)

def generate_query(n_clicks, direction, days, sd_choice, cancel_choice, option=default_options):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate
    else:
        selected_days = get_days(days)
        if option != default_options:
            filter_query_string = get_query_string(option)
            query = dedent(
                f"""
                SELECT
                    t.direction AS "Direction",
                    t.station_code AS "Station",
                    t.sb_mile,
                    t.arrival_or_departure AS "Arrival or Departure",
                    CAST(AVG(t.timedelta_from_sched) AS INTEGER) AS "Average Delay",
                    COUNT(*) AS "Num Records"
                FROM
                    stops_joined t
                WHERE
                    t.direction = {direction} AND
                    t.sched_arr_dep_week_day IN {selected_days} AND
                    t.service_disruption = {sd_choice} AND
                    t.cancellations = {cancel_choice} 
                """ + "AND " + filter_query_string + 
                """
                GROUP BY t.station_code, t.direction, t.sb_mile, t.arrival_or_departure
                ORDER BY t.sb_mile ASC;
                """
            )
        else:
            query = dedent(
                f"""
                SELECT
                    t.direction AS "Direction",
                    t.station_code AS "Station",
                    t.sb_mile,
                    t.arrival_or_departure AS "Arrival or Departure",
                    CAST(AVG(t.timedelta_from_sched) AS INTEGER) AS "Average Delay",
                    COUNT(*) AS "Num Records"
                FROM
                    stops_joined t
                WHERE
                    t.direction = {direction} AND
                    t.sched_arr_dep_week_day IN {selected_days} AND
                    t.service_disruption = {sd_choice} AND
                    t.cancellations = {cancel_choice} 
                GROUP BY t.station_code, t.direction, t.sb_mile, t.arrival_or_departure
                ORDER BY t.sb_mile ASC;
                """
            )
        print(query)
        try:
            t0 = time.time()
            query_df = connect_and_query(query)
            assert query_df.shape[0] > 10
        except AssertionError:
            raise dash.exceptions.PreventUpdate

        colors, delays, color_group_key = get_colors(geo_route, query_df)
        route = px.line_mapbox(
            geo_route,
            lat=geo_route['Latitude'],
            lon=geo_route['Longitude'],
            line_group=geo_route['Connecting Path'],
            color=geo_route[color_group_key],
            color_discrete_map=colors,
            hover_data={color_group_key: False, 'Group': False},
            mapbox_style=map_style,
            zoom=6.15)
        route.update_traces(line=dict(width=3))
        route.add_trace(go.Scattermapbox(
            lat=geo_info.LAT.round(decimals=5),
            lon=geo_info.LON.round(decimals=5),
            name='Amtrak Stations',
            hoverinfo='text',
            customdata=delays,
            hovertext=geo_info['STNNAME'],
            hovertemplate="%{hovertext} (Avg. Delay: %{customdata} mins)<extra></extra>",
            mode='markers',
            marker={'size': 6, 'color': contrast_color},
            fill='none'
            )
        )
        route.update_layout(
            dict(
                paper_bgcolor="white",
                plot_bgcolor="white",
                margin=dict(t=35, l=80, b=0, r=0)))
        route.update_yaxes(automargin=True)
        t1 = time.time()
        exec_time = t1 - t0
        query_size = query_df["Num Records"].sum()
        alert_msg = f"Queried {query_size} records. Total time: {exec_time:.2f}s."
        alert = dbc.Alert(alert_msg, color="success", dismissable=True)
    return alert, route


if __name__ == '__main__':
    app.run_server(debug=True)
