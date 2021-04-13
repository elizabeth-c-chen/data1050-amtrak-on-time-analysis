import os
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
import plotly


# Helper Functions
def connect_and_query(query):
    db_string = "dbname='amtrakproject' user='appuser' password="
    conn = psycopg2.connect(db_string + os.environ.get('DB_PASS'))
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


def get_precip(precip_selected):
    output = '('
    precip_types = ['Rain', 'Snow', 'No Precipitation']
    for i in range(len(precip_types)):
        if i in precip_selected:
            output += '\'' + precip_types[i] + '\'' + ', '
    output = output[0:-2] + ')'
    return output


def get_continuous_color(colorscale, intermed):
    """
    Plotly continuous colorscales assign colors to the range [0, 1]. This function computes
    the intermediate color for any value in that range.

    Plotly doesn't make the colorscales directly accessible in a common format.
    Some are ready to use:
        colorscale = plotly.colors.PLOTLY_SCALES["Greens"]

    Others are just swatches that need to be constructed into a colorscale:
     colors, scale = plotly.colors.convert_colors_to_same_type(plotly.colors.sequential.Viridis)
     colorscale = plotly.colors.make_colorscale(colors, scale=scale)

    :param colorscale: A plotly continuous colorscale defined with RGB string colors.
    :param intermed: value in the range [0, 1]
    :return: color in rgb string format
    :rtype: str
    """
    if len(colorscale) < 1:
        raise ValueError("colorscale must have at least one color")

    if intermed <= 0 or len(colorscale) == 1:
        return colorscale[0][1]
    if intermed >= 1:
        return colorscale[-1][1]

    for cutoff, color in colorscale:
        if intermed > cutoff:
            low_cutoff, low_color = cutoff, color
        else:
            high_cutoff, high_color = cutoff, color
            break

    return plotly.colors.find_intermediate_color(
        lowcolor=low_color, highcolor=high_color,
        intermed=((intermed - low_cutoff) / (high_cutoff - low_cutoff)),
        colortype="rgb")


def get_colors(query_df):
    """
    Create the path color groups according to data from query.
    """
    direction = query_df['Direction'].iloc[0]
    colors, scale = plotly.colors.convert_colors_to_same_type(plotly.colors.sequential.Turbo)
    colorscale = plotly.colors.make_colorscale(colors, scale=scale)
    if direction == 'Northbound':
        color_group_key = 'NB Station Group'
    elif direction == 'Southbound':
        color_group_key = 'SB Station Group'
    station_column = geo_route[color_group_key]
    colors = {station: 'rgb(0,0,0)' for station in station_column.unique()}
    scaled_delay = query_df['Average Delay'] / query_df['Average Delay'].max()
    for station in query_df['Station']:
        delay_val = scaled_delay.loc[query_df['Station'] == station].values[0]
        delay_color = get_continuous_color(colorscale, delay_val)
        colors[station] = delay_color
    return colors, query_df['Average Delay'], color_group_key


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
        station_code AS "STNCODE",
        station_name as "STNNAME",
        longitude as "LON",
        latitude as "LAT"
    FROM
        station_info
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
                d.direction AS "Direction",
                d.station_code AS "Station",
                ROUND(AVG(d.depart_diff), 2) AS "Average Delay",
                COUNT(*) AS "Num Records"
            FROM
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
                    wh.conditions IN ('Rain', 'Snow', 'No Precipitation')
                ) wh ON wh.station_code = d.station_code AND
                DATE_TRUNC('hour', d.full_sched_dep_datetime) = wh.date_time
            WHERE
                d.direction = 'Southbound' AND
                d.origin_week_day IN
                    ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday')
                AND
                d.depart_diff BETWEEN 0 AND 100 AND
                d.service_disruption = '0' AND
                d.cancellations = '0'
            GROUP BY d.station_code, d.direction;
            """
)
default_query_df = connect_and_query(default_query)
colors, delays, color_group_key = get_colors(default_query_df)

# Info for map -- change later
amtrak_stations = list(geo_info['STNCODE'])
location_names = list(geo_info['STNNAME'])
map_style = 'outdoors'

# Route Visualization with Stand-in Color Coded Groups
route = px.line_mapbox(geo_route,
                       lat=geo_route['Latitude'],
                       lon=geo_route['Longitude'],
                       line_group=geo_route['Connecting Path'],
                       color=geo_route[color_group_key],
                       color_discrete_map=colors,
                       hover_data={color_group_key: False, 'Group': False},
                       mapbox_style=map_style,
                       zoom=6)
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

controls = dbc.Card(
    [
        dbc.FormGroup(
            [
                dbc.Label('Choose a direction for travel'),
                dcc.RadioItems(
                    id='direction-selector',
                    options=[{'label': 'Northbound', 'value': "\'Northbound\'"},
                             {'label': 'Southbound', 'value': "\'Southbound\'"}],
                    value="\'Southbound\'"
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
                            {'label': 'Wednesday', 'value': 3},
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
                dcc.Checklist(
                    id='weather-conditions-selector',
                    options=[
                            {'label': 'Rain', 'value': 0},
                            {'label': 'Snow', 'value': 1},
                            {'label': 'No Precipitation', 'value': 2}
                    ],
                    value=[0, 1, 2]
                )
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Enter a range of allowed delays in minutes (max = 600)'),
                dcc.Input(id='min-delay', type='number', value=0, min=0, max=599),
                dcc.Input(id='max-delay', type='number', value=100, min=1, max=600)
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Allow data with known Service Disruptions in query?'),
                dcc.RadioItems(
                    id='allow-sd-choice',
                    options=[
                        {'label': 'Yes', 'value': "\'1\'"},
                        {'label': 'No', 'value': "\'0\'"}],
                    value="\'0\'")
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Allow data with known Cancellations in query?'),
                dcc.RadioItems(
                    id='allow-cancel-choice',
                    options=[
                        {'label': 'Yes', 'value': "\'1\'"},
                        {'label': 'No', 'value': "\'0\'"}
                    ],
                    value="\'0\'")
            ]
        ),
        dbc.Button(
            "Submit Query and Plot Results",
            color="primary",
            id='send-query-button',
            style={'font-size': '14px'}
        )
    ],
    body=True
)

viz = dbc.Card(
    [
        dcc.Graph(
            id='geo-route',
            config=config,
            figure=route)
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
                dbc.Col(controls, md=4, lg=3.25),
                dbc.Col(viz, md=8, lg=8.75)
            ],
            no_gutters=False
        )
    ],
    fluid=True,
)


@app.callback(
    Output("geo-route", "figure"),
    [Input("send-query-button", 'n_clicks')],
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
def generate_query(n_clicks, direction, days, weather, min_d, max_d, sd, cancel):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate
    else:
        selected_days = get_days(days)
        selected_precip = get_precip(weather)
        query = dedent(
            f"""
            SELECT
                d.direction AS "Direction",
                d.station_code AS "Station",
                ROUND(AVG(d.depart_diff), 2) AS "Average Delay",
                COUNT(*) AS "Num Records"
            FROM
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
                    wh.conditions IN {selected_precip}
                ) wh ON wh.station_code = d.station_code AND
                DATE_TRUNC('hour', d.full_sched_dep_datetime) = wh.date_time
            WHERE
                d.direction = 'Southbound' AND
                d.origin_week_day IN {selected_days} AND
                d.depart_diff BETWEEN {min_d} AND {max_d} AND
                d.service_disruption = {sd} AND
                d.cancellations = {cancel}
            GROUP BY d.station_code, d.direction;
            """
        )
        try:
            query_df = connect_and_query(query)
            assert query_df.shape[0] > 5
        except AssertionError:
            raise dash.exceptions.PreventUpdate
        colors, delays, color_group_key = get_colors(query_df)
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
    return route


if __name__ == '__main__':
    app.run_server(debug=True)
