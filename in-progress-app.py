# Run this app with `python in-progress-app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import geopandas as gpd
import os

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

mapbox_token = os.environ.get('MAPBOX_TOKEN')
assert mapbox_token is not None, 'empty token'

geo_stations = gpd.read_file('./data/geojson/Amtrak_Project_Stations.geojson')
geo_route = pd.read_csv('./data/visualization/NE_regional_lonlat.csv')

amtrak_stations = ['BOS', 'BBY', 'RTE', 'PVD', 'KIN', 'NLC',
                   'NHV', 'STM', 'NYP', 'NWK', 'TRE', 'PHL',
                   'WIL', 'BAL', 'BWI', 'NCR', 'WAS']

location_names = list(geo_stations['STNNAME'])

train_nums = [66, 67, 82, 83, 86, 88, 93, 94, 95, 99, 132, 135, 137, 139,
              150, 160, 161, 162, 163, 164, 165, 166, 167, 168, 170, 171,
              172, 173, 174, 175, 195]

# map_style = 'mapbox://styles/elizabethchen/ckmyomzg920pz17o6n7hg2lnd'
map_style = 'outdoors'

px.set_mapbox_access_token(mapbox_token)

stations = px.scatter_mapbox(geo_stations,
                             lat=geo_stations.geometry.y,
                             lon=geo_stations.geometry.x,
                             mapbox_style=map_style,
                             color_discrete_sequence=['Navy'],
                             size_max=25,
                             hover_name="STNNAME",
                             center=dict(lat=40.58, lon=-74.00),
                             zoom=5.5,
                             height=500,
                             width=800)

route = px.line_mapbox(geo_route,
                       lat=geo_route['Latitude'],
                       lon=geo_route['Longitude'],
                       line_group=geo_route['Group'],
                       color=geo_route['Group'],
                       color_discrete_sequence=px.colors.qualitative.Dark24,
                       hover_data={'Latitude': True,
                                   'Longitude': True,
                                   'Group': False},
                       mapbox_style=map_style,
                       zoom=5.5,
                       height=500,
                       width=800)

route.update_traces(line=dict(width=3))
config = dict({'scrollZoom': False})
# https://plotly.github.io/plotly.py-docs/generated/plotly.express.scatter_mapbox.html
# info for coloring the line_mapbox path ^^ (reminder for later)

app.layout = html.Div(children=[
    html.H1(children='Amtrak Northeast Regional On-Time Performance Analysis'),
    html.H3(children='''
        DATA 1050 Final Project by Elizabeth Chen
    '''),
    html.B(id='text-stations-choice',
           children='1. Select a range of stations for travel between.'),
    dcc.RangeSlider(
        id='station-selector',
        marks={i: amtrak_stations[i] for i in range(len(amtrak_stations))},
        min=0,
        max=len(amtrak_stations)-1,
        value=[3, 8],
        persistence=True
    ),
    html.Div(id='slider-output-container'),
    html.B(id='text-trains-choice',
           children='2. Select one or more trains from the dropdown menu.'),
    dcc.Dropdown(
        id='train-num-selector',
        options=[dict(label=num, value=num) for num in train_nums],
        placeholder='Select one or more train numbers',
        value=['163'],
        multi=True
    ),
    html.B(id='text-trains-or-direction-choice',
           children='''Alternatively, choose one direction to
                       select all Northbound or Southbound trains.'''),
    dcc.RadioItems(
        id='direction-selector',
        options=[
            {'label': 'Northbound', 'value': 'Northbound'},
            {'label': 'Southbound', 'value': 'Southbound'},
            {'label': 'No choice', 'value': 'None'}
        ],
        value='None'
    ),
    html.B(id='text-days-of-week-choice',
           children='3. Select one or more days of the week to include.'),
    dcc.Checklist(
        id='day-of-week-selector',
        options=[
            {'label': 'Monday', 'value': 'op_on_monday'},
            {'label': 'Tuesday', 'value': 'op_on_tuesday'},
            {'label': 'Wednesday', 'value': 'op_on_wednesday'},
            {'label': 'Thursday', 'value': 'op_on_thursday'},
            {'label': 'Friday', 'value': 'op_on_friday'},
            {'label': 'Saturday', 'value': 'op_on_saturday'},
            {'label': 'Sunday', 'value': 'op_on_sunday'}
        ],
        value=['op_on_monday', 'op_on_tuesday',
               'op_on_wednesday', 'op_on_thursday',
               'op_on_friday', 'op_on_saturday', 'op_on_sunday'],
        labelStyle={'display': 'inline-block'}
    ),
    html.B(id='text-service-disruptions-choice',
           children='4. Allow data with known service disruptions?'),
    dcc.RadioItems(
        id='allow-service-disruptions-choice',
        options=[
            {'label': 'Yes', 'value': '1'},
            {'label': 'No', 'value': '0'}
        ],
        value='0'
    ),
    html.B(id='text-select-year-quarter',
           children='5. Select seasons to include in the analysis.'),
    dcc.Checklist(
        id='quarter-selector',
        options=[
            {'label': 'Fall', 'value': '4'},
            {'label': 'Winter', 'value': '1'},
            {'label': 'Spring', 'value': '2'},
            {'label': 'Summer', 'value': '3'}
        ],
        value=['4', '1', '2', '3'],
        labelStyle={'display': 'inline-block'}
    ),
    dcc.Graph(
        id='geo-stations',
        config=config,
        figure=stations
    ),
    dcc.Graph(
        id='geo-route',
        config=config,
        figure=route
    )
])


@app.callback(
    dash.dependencies.Output('slider-output-container', 'children'),
    [dash.dependencies.Input('station-selector', 'value')])
def update_output(value):
    starts, ends = location_names[value[0]], location_names[value[1]]
    return 'Stations between "{}" and "{}" are selected'.format(starts, ends)


if __name__ == '__main__':
    app.run_server(debug=True)
