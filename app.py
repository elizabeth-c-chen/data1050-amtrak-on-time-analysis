# Run this app with `python unfinished-app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

app = dash.Dash(__name__)
server = app.server
app.title = 'DATA 1050 Project'

mapbox_token = os.environ.get('MAPBOX_TOKEN')
assert mapbox_token is not None, 'empty token'
px.set_mapbox_access_token(mapbox_token)

geo_route = pd.read_csv('./data/visualization/NE_regional_lonlat.csv')
geo_info = pd.read_csv('./data/visualization/geo_stations_info.csv')

amtrak_stations = ['BOS', 'BBY', 'RTE', 'PVD', 'KIN', 'NLC',
                   'NHV', 'STM', 'NYP', 'NWK', 'TRE', 'PHL',
                   'WIL', 'BAL', 'BWI', 'NCR', 'WAS']

location_names = list(geo_info['STNNAME'])

train_nums = [66, 67, 82, 83, 86, 88, 93, 94, 95, 99, 132, 135, 137, 139,
              150, 160, 161, 162, 163, 164, 165, 166, 167, 168, 170, 171,
              172, 173, 174, 175, 195]

map_style = 'outdoors'

geo_route_path = geo_route['Connecting Path']
route = px.line_mapbox(geo_route,
                       lat=geo_route['Latitude'],
                       lon=geo_route['Longitude'],
                       line_group=geo_route['Group'],
                       color=geo_route_path,
                       color_discrete_sequence=px.colors.qualitative.T10,
                       hover_data={'Group': False},
                       mapbox_style=map_style,
                       zoom=6)
route.update_traces(line=dict(width=3))

route.add_trace(go.Scattermapbox(lat=geo_info.LAT.round(decimals=5),
                                 lon=geo_info.LON.round(decimals=5),
                                 name='Amtrak Stations',
                                 hoverinfo='text',
                                 customdata=geo_info.STNCODE,
                                 hovertext=geo_info.STNNAME,
                                 hovertemplate="%{hovertext} (%{customdata})<extra></extra>",
                                 mode='markers',
                                 marker={'size': 6, 'color': 'Navy'},
                                 fill='none'
                                 )
                )

route.update_layout(dict(paper_bgcolor="white", plot_bgcolor="white", margin=dict(t=35, l=80, b=0, r=0), height=500)) # l=0, r=0
route.update_yaxes(automargin=True)

config = dict({'scrollZoom': False})

app.layout = html.Div(children=[
    html.H3(children='Amtrak Northeast Regional On-Time Performance Analysis'),
    html.H5(children='An In-Progress DATA 1050 Final Project by Elizabeth C. Chen'),
    html.H6(children='''
        More features and interactive data analysis tools are on the way!
    '''),
  #  html.B(id='text-stations-choice',
  #         children='Select a range of stations for travel between.'),
    html.Div([
        dcc.RangeSlider(
            id='station-selector',
            marks={i: amtrak_stations[i] for i in range(len(amtrak_stations))},
            min=0,
            max=len(amtrak_stations)-1,
            value=[3, 8],
            persistence=True
        )
        ],
        style={'padding-left': '20%', 'padding-right': '20%', 'width': '60%'} # 'align': 'center',
    ),
    html.Div(id='slider-output-container', style={'text-align': 'center', 'font-weight': 'Bold'}),
#    html.B(id='text-trains-choice',
#           children='2. Select one or more trains from the dropdown menu. Even numbers correspond to Northbound trains, odd numbers correspond to Southbound trains.'),
#    dcc.Dropdown(
#        id='train-num-selector',
#        options=[dict(label=num, value=num) for num in train_nums],
#        placeholder='Select one or more trains (not all trains are listed here).',
#        value=['163'],
#        multi=True
#    ),
  #  html.B(id='text-trains-or-direction-choice',
  #         children='''Alternatively, choose one direction to
  #                     select all Northbound or Southbound trains.'''),
  #  dcc.RadioItems(
  #      id='direction-selector',
  #      options=[
  #          {'label': 'Northbound', 'value': 'Northbound'},
  #          {'label': 'Southbound', 'value': 'Southbound'},
  #          {'label': 'No choice', 'value': 'None'}
  #      ],
  #      value='None'
  #  ),
  #  html.B(id='text-days-of-week-choice',
  #         children='3. Select one or more days of the week to include.'),
  #  dcc.Checklist(
  #      id='day-of-week-selector',
  #      options=[
  #          {'label': 'Monday', 'value': 'op_on_monday'},
  #          {'label': 'Tuesday', 'value': 'op_on_tuesday'},
  #          {'label': 'Wednesday', 'value': 'op_on_wednesday'},
  #          {'label': 'Thursday', 'value': 'op_on_thursday'},
  #          {'label': 'Friday', 'value': 'op_on_friday'},
  #          {'label': 'Saturday', 'value': 'op_on_saturday'},
  #          {'label': 'Sunday', 'value': 'op_on_sunday'}
  #      ],
  #      value=['op_on_monday', 'op_on_tuesday',
  #             'op_on_wednesday', 'op_on_thursday',
  #             'op_on_friday', 'op_on_saturday', 'op_on_sunday'],
  #      labelStyle={'display': 'inline-block'}
  #  ),
  #  html.B(id='text-service-disruptions-choice',
  #         children='4. Allow data with known service disruptions?'),
  #  dcc.RadioItems(
  #      id='allow-service-disruptions-choice',
  #      options=[
  #          {'label': 'Yes', 'value': '1'},
  #          {'label': 'No', 'value': '0'}
  #      ],
  #      value='0'
  #  ),
  #  html.B(id='text-select-year-quarter',
  #         children='5. Select seasons to include in the analysis.'),
  #  dcc.Checklist(
  #      id='quarter-selector',
  #      options=[
  #          {'label': 'Fall', 'value': '4'},
  #          {'label': 'Winter', 'value': '1'},
  #          {'label': 'Spring', 'value': '2'},
  #          {'label': 'Summer', 'value': '3'}
  #      ],
  #      value=['4', '1', '2', '3'],
  #      labelStyle={'display': 'inline-block'}
  #  ),
    dcc.Graph(
        id='geo-route',
        config=config,
        figure=route,
        style={'display': 'block', 'margin-left': '19%', 'margin-right': '15%'} # 'margin-left': 'auto', 'margin-right': 'auto', 'margin-top': 'auto', 'margin-bottom': 'auto',
    ),
    html.Div(children=[
        html.Footer(children=[
            html.P(children="You are visiting the portfolio of Elizabeth C. Chen, Master's student at Brown University. This webpage is not affiliated with Amtrak in any way.", style={'font-size': 12})]
        )
    ], style={'display': 'block', 'padding-left': '26%', 'padding-right': '24%'})
])


@app.callback(
    dash.dependencies.Output('slider-output-container', 'children'),
    [dash.dependencies.Input('station-selector', 'value')])
def update_output(value):
    starts, ends = location_names[value[0]], location_names[value[1]]
    return 'Stations between {} and {} are selected.'.format(starts, ends)


if __name__ == '__main__':
    app.run_server(debug=True)
