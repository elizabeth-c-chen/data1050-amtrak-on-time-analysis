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

from utils import connect_and_query, get_days, get_precip, get_colors

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
map_style = 'outdoors'

# Route Visualization with Stand-in Color Coded Groups
route = px.line_mapbox(geo_route,
                       lat=geo_route['Latitude'],
                       lon=geo_route['Longitude'],
                       line_group=geo_route['Connecting Path'],
                       color=geo_route[color_group_key],
                       color_discrete_map=colors_dict,
                       hover_data={color_group_key: False, 'Group': False},
                       mapbox_style=map_style,
                       zoom=5.75)
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
                                 marker={'size': 6, 'color': 'Navy'},
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
                    style={'font-size': 14, 'padding-left': '5%'}
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
                    style={'font-size': 14, 'padding-left': '5%'}
                )
            ]
        ),
  #      dbc.FormGroup(
  #          [
  #              dbc.Label('Select one or more precipitation conditions to include', style={'font-size': 15}),
  #              dcc.Checklist(
  #                  id='weather-conditions-selector',
  #                  options=[
  #                          {'label': 'Rain', 'value': 0},
  #                          {'label': 'Snow', 'value': 1},
  #                          {'label': 'No Precipitation', 'value': 2}
  #                  ],
  #                  value=[0, 1, 2],
  #                  inputStyle={"margin-right": "10px"},
  #                  style={'font-size': 14, 'padding-left': '5%'}
  #              )
  #          ]
  #      ),
#        dbc.FormGroup(
#            [
#                dbc.Label('Enter a range of allowed delays in minutes (max = 600)'),
#                dcc.Input(id='min-delay', type='number', value=0, min=0, max=599),
#                dcc.Input(id='max-delay', type='number', value=100, min=1, max=600)
#            ]
#        ),
#        dbc.FormGroup(
#            [
#                dbc.Label('Allow data with known Service Disruptions in query?'),
#                dcc.RadioItems(
#                    id='allow-sd-choice',
#                    options=[
#                        {'label': 'Yes', 'value': "\'1\'"},
#                        {'label': 'No', 'value': "\'0\'"}],
#                    value="\'0\'")
#            ]
#        ),
#        dbc.FormGroup(
#            [
#                dbc.Label('Allow data with known Cancellations in query?'),
#                dcc.RadioItems(
#                    id='allow-cancel-choice',
#                    options=[
#                        {'label': 'Yes', 'value': "\'1\'"},
#                        {'label': 'No', 'value': "\'0\'"}
#                    ],
#                    value="\'0\'")
#            ]
#        ),
        dbc.Button(
            "Submit Query and Plot Results",
            color="primary",
            id='send-query-button',
            style={'font-size': 15}
        )
    ],
    body=True
)

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
        html.H5("A DATA 1050 Final Project by Elizabeth C. Chen"),
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


@app.callback(
    [
        Output("alert-msg", "children"),
        Output("geo-route", "figure")
    ],
    [
        Input("send-query-button", 'n_clicks')
    ],
    [
        State('direction-selector', 'value'),
        State('days-of-week-checkboxes', 'value')#,
   #     State('weather-conditions-selector', 'value'),
#        State('min-delay', 'value'),
#        State('max-delay', 'value'),
#        State('allow-sd-choice', 'value'),
#        State('allow-cancel-choice', 'value')
    ]
)
def generate_query(n_clicks, direction, days): #, weather):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate
    else:
        selected_days = get_days(days)
             #   selected_precip = get_precip(weather) 
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
                t.sched_arr_dep_week_day IN {selected_days}
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
            zoom=6)
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
            marker={'size': 6, 'color': 'Navy'},
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
