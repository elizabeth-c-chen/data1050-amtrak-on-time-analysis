import os
from textwrap import dedent
import time
from datetime import date, timedelta, datetime

import numpy as np
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State

import plotly.express as px
import plotly.graph_objects as go

from utils import connect_and_query, get_colors, get_days, get_precip_types, get_sort_from_train_num, get_sort_from_direction

######################
# DATABASE SETUP
######################
assert os.environ.get('DATABASE_URL') is not None, 'database URL is not set!'

######################
# DASH SETUP
######################
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    update_title=None
)
server = app.server
app.title = "DATA 1050 Final Project"

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

######################
# MAP SETUP
######################
assert os.environ.get('MAPBOX_TOKEN') is not None, 'empty token'
px.set_mapbox_access_token(os.environ.get('MAPBOX_TOKEN'))

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

amtrak_stations = list(geo_info['STNCODE'])
location_names = list(geo_info['STNNAME'])

######################
# STYLING
######################
OPTION_LABEL_STYLE_WITH_DOWN_MARGIN = {'font-size': 15, 'margin-bottom': '2.5%'}
OPTION_LABEL_STYLE_WITH_UP_DOWN_MARGIN = {'font-size': 15, 'margin-bottom': '2.5%', 'margin-top': '2.5%'}
OPTION_VIEWABLE_FALSE = {'font-size': 15, 'display': 'none'}
OPTION_STYLE = {'font-size': 14, 'padding-left': '5%'}
OPTION_LABEL_STYLE = {'font-size': 15}

VIEWABLE_CARD_STYLE = {'display': 'block'}
HIDDEN_CARD_STYLE = {'display': 'none', 'padding-top': '5%'}
ERROR_CARD_LABEL_STYLE = {'font-size': 15, 'display': 'block', 'color': 'red'}
BUTTON_STYLE = {'font-size': 15, 'margin-top': '2.5%'}


FIGURE_STYLE = {'height': 550, 'width': 950}
MAPBOX_STYLE = 'mapbox://styles/elizabethchen/ckpwqldby4ta317nj9xfc1eeu'
CONTRAST_COLOR = 'navy'
MARKER_STYLE = {'size': 6, 'color': CONTRAST_COLOR}
PATH_STYLE = {'width': 3.5}
ZOOM_LEVEL = 6.0
FIGURE_LAYOUT_STYLE = dict(paper_bgcolor='white', plot_bgcolor='white', margin=dict(t=35, l=80, b=0, r=0))


######################
# VISUALIZATION PAGE 
######################


default_query = dedent(
            """
            SELECT
                direction AS "Direction",
                station_code AS "Station",
                sb_stop_num AS "Stop Number",
                arrival_or_departure AS "Arrival or Departure",
                CAST(AVG(timedelta_from_sched) AS INTEGER) AS "Average Delay",
                COUNT(*) AS "Num Records"
            FROM
                stops_joined
            WHERE
                direction = 'Southbound' AND
                sched_arr_dep_week_day IN
                    ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday') 
            GROUP BY station_code, direction, sb_stop_num, arrival_or_departure
            ORDER BY sb_stop_num ASC;
            """
)

default_query_df = connect_and_query(default_query)
colors_dict, delays, counts, color_group_key = get_colors(geo_route, default_query_df)

# Route Visualization with Stand-in Color Coded Groups
route = px.line_mapbox(geo_route,
                       lat=geo_route['Latitude'],
                       lon=geo_route['Longitude'],
                       line_group=geo_route['Connecting Path'],
                       color=geo_route[color_group_key],
                       color_discrete_map=colors_dict,
                       hover_data={color_group_key: False, 'Group': False},
                       mapbox_style=MAPBOX_STYLE,
                       zoom=ZOOM_LEVEL)

route.update_traces(line=dict(width=3))

route.add_trace(
    go.Scattermapbox(
        lat=geo_info['LAT'].round(decimals=5),
        lon=geo_info['LON'].round(decimals=5),
        name='Amtrak Stations',
        hoverinfo='text',
        customdata=np.stack([delays, counts], axis=-1),
        hovertext=np.stack([geo_info['STNNAME'], geo_info['STNCODE']], axis=-1),
        hovertemplate="""
                    %{hovertext[0]} (%{hovertext[1]}) <br>
                    Avg. Delay: %{customdata[0]} mins
                    (<i>n</i> = %{customdata[1]})<extra></extra>""",
        mode='markers',
        marker=MARKER_STYLE,
        fill='none'
    )
)
route.update_layout(FIGURE_LAYOUT_STYLE)

route.update_yaxes(automargin=True)

config = dict({'scrollZoom': False})

div_alert = html.Div(
    dbc.Alert(
        "Showing results from default selection.",
        color="info", 
        dismissable=True
    ), 
    id="alert-msg"
)
controls = dbc.Card(
    [
        html.H6(html.B("Query Settings")),
        html.P(
            html.B(
                "Changing the options below will modify the data selected for plotting.",
                style={'font-size': 14}
            )
        ),
        dbc.FormGroup(
            [
                dbc.Label('Choose a direction for travel', style=OPTION_LABEL_STYLE),
                dcc.RadioItems(
                    id='direction-selector',
                    options=[{'label': 'Northbound', 'value': "\'Northbound\'"},
                             {'label': 'Southbound', 'value': "\'Southbound\'"}],
                    value="\'Southbound\'",
                    inputStyle={"margin-right": "10px"},
                    style=OPTION_STYLE,
                    persistence=True,
                    persistence_type='session'
                ),
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Select one or more days of the week to include', style=OPTION_LABEL_STYLE),
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
                    style=OPTION_STYLE,
                )
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label('Select one or more precipitation conditions to include', style=OPTION_LABEL_STYLE),
                dcc.Checklist(
                    id='weather-type',
                    options=[
                            {'label': 'No Precipitation', 'value': 'None'},
                            {'label': 'Rain', 'value': 'Rain'},
                            {'label': 'Snow', 'value': 'Snow'},
                            {'label': 'Other', 'value': 'Other'}
                    ],
                    value=['None', 'Rain', 'Snow', 'Other'],
                    inputStyle={"margin-right": "10px"},
                    style=OPTION_STYLE
                )
            ]
        ),
        dbc.Button(
            "Submit Query and Plot Results",
            id="send-query-button",
            color="primary",
            style=BUTTON_STYLE
        )
    ],
    id="controls",
    body=True
)

viz = dbc.Card(
    [
        dcc.Graph(
            id='geo-route',
            config=config,
            figure=route,
            style=FIGURE_STYLE
        )
    ],
    body=True
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
        State('days-of-week-checkboxes', 'value'),
        State('weather-type', 'value')
    ]
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
        print(query)
        try:
            t0 = time.time()
            query_df = connect_and_query(query)
            print(query_df)
            assert query_df.shape[0] > 0
        except AssertionError:
            raise dash.exceptions.PreventUpdate

        colors, delays, counts, color_group_key = get_colors(geo_route, query_df)
        route = px.line_mapbox(
            geo_route,
            lat=geo_route['Latitude'],
            lon=geo_route['Longitude'],
            line_group=geo_route['Connecting Path'],
            color=geo_route[color_group_key],
            color_discrete_map=colors,
            hover_data={color_group_key: False, 'Group': False},
            mapbox_style=MAPBOX_STYLE,
            zoom=ZOOM_LEVEL)
        route.update_traces(line=dict(width=3))
        route.add_trace(go.Scattermapbox(
            lat=geo_info.LAT.round(decimals=5),
            lon=geo_info.LON.round(decimals=5),
            name='Amtrak Stations',
            hoverinfo='text',
            customdata=np.stack([delays, counts], axis=-1),
            hovertext=np.stack([geo_info['STNNAME'], geo_info['STNCODE']], axis=-1),
            hovertemplate="""
                        %{hovertext[0]} (%{hovertext[1]}) <br>
                        Avg. Delay: %{customdata[0]} mins
                        (<i>n</i> = %{customdata[1]})<extra></extra>""",
            mode='markers',
            marker=MARKER_STYLE,
            fill='none'
            )
        )
        route.update_layout(FIGURE_LAYOUT_STYLE)
        route.update_yaxes(automargin=True)
        t1 = time.time()
        exec_time = t1 - t0
        query_size = int(counts.sum())
        alert_msg = f"Queried {query_size} records. Total time: {exec_time:.2f}s."
        alert = dbc.Alert(alert_msg, color="success", dismissable=True)
    return alert, route


######################
# ENHANCEMENT PAGE
######################
specific_trip_controls = dbc.Card(
    [
        html.H6(html.B("Query Settings")),
        html.P(
            html.B(
                "Start by selecting date from the calendar below.",
                style={'font-size': 14}
            )
        ),
        dbc.Label("1. Select a date to view past train trips.", style=OPTION_LABEL_STYLE_WITH_DOWN_MARGIN),
        dcc.DatePickerSingle(
            id="single-trip-date-picker",
            display_format="MMMM Do, YYYY",
            min_date_allowed=date(2011, 1, 1),
            max_date_allowed=date.today()-timedelta(days=1),
            initial_visible_month=date.today(),
            placeholder="Select a date",
            with_full_screen_portal=True,
            persistence=True
        ),
        html.Div(
            [
                dbc.Label("2. Select a train trip from the selected date.", id="step-2-label", style=OPTION_LABEL_STYLE_WITH_UP_DOWN_MARGIN),
                dcc.Dropdown(
                    disabled=False,
                    id='train-num-picker',
                    searchable=True,
                    clearable=False,
                    placeholder="Select a train number"
                )
            ],
            id='step-2-container',
            style=HIDDEN_CARD_STYLE
        ),
        html.Div(
            [
                dbc.Label(
                    "3. Select a range of years to compare this trip with historical data.",
                    id="step-3-label", style=OPTION_LABEL_STYLE_WITH_UP_DOWN_MARGIN),
                dcc.RangeSlider(
                    min=2011,
                    max=2021,
                    value=[2011, 2019],
                    marks={
                        year: {'label': str(year)} for year in range(2011, 2022)
                    },
                    id="historical-range-slider"
                )
            ],
            id='step-3-container',
            style=HIDDEN_CARD_STYLE
        ),
        dbc.Button(
            "Submit Query and View Results",
            id="enhancement-send-query-button",        
            color="primary",
            style=BUTTON_STYLE,
            disabled=True
        )
    ],
    body=True
)
hypo_test_view_initial = dbc.Card(
    [
        html.P("Select a date and train to compare with its historical averages.")
    ],
    id="enhancement-view"
)

#hypo_test_view = dbc.Card(
#[]
#)


@app.callback(
    [
        Output("step-2-container", "style"),
        Output("train-num-picker", "options")
    ],
    [
        Input("single-trip-date-picker", "date")
    ]
)
def show_step2(date_value):
    if date_value is not None:
        avail_trains_query = f"""
                              SELECT DISTINCT train_num 
                              FROM stops_joined 
                              WHERE sched_arr_dep_date = '{date_value}'"""
        result = connect_and_query(avail_trains_query)
        if result.shape[0] > 0:
            step2_style = VIEWABLE_CARD_STYLE
            train_num_options = [
                {'label': train_num, 'value': train_num} for train_num in result['train_num']
            ]
            return step2_style, train_num_options
        else:
            raise dash.exceptions.PreventUpdate
    else:
        raise dash.exceptions.PreventUpdate
        

@app.callback(
    [
        Output("step-3-container", "style"),
        Output("enhancement-send-query-button", "disabled")
    ],
    [
        Input("train-num-picker", "value")
    ]
)
def show_step3(train_num_selected):
    if train_num_selected is not None:
        step3_style = VIEWABLE_CARD_STYLE
        button_disabled = False
        return step3_style, button_disabled
    else:
        raise dash.exceptions.PreventUpdate
    


@app.callback(
    Output("enhancement-view", "children"),
    [
        Input("enhancement-send-query-button", "n_clicks")
    ],
    [
        State("single-trip-date-picker", "date"),
        State("train-num-picker", "value"),
        State("historical-range-slider", "value")
    ]
)
def enable_send_query(n_clicks, selected_date, train_num, year_range):
    if n_clicks is not None:
        sort_stop_num = get_sort_from_train_num(train_num)
        single_trip_query = f"""
                             SELECT
                                direction AS "Direction",
                                station_code AS "Station",
                                {sort_stop_num} AS "Stop Number",
                                service_disruption AS "Service Disruption",
                                cancellations AS "Cancellations",
                                arrival_or_departure AS "Arrival or Departure",
                                CAST(timedelta_from_sched AS integer) AS "Delay"
                             FROM
                                 stops_joined
                             WHERE
                                 origin_date = '{selected_date}' AND
                                 train_num = '{train_num}'
                             ORDER BY {sort_stop_num} ASC, arrival_or_departure ASC;
                             """
     #  sd_occurred = single_trip_query["Service Disruption"].astype(int)
    #    print(sd_occurred)
        historical_query = f"""
                            SELECT
                                station_code AS "Station",
                                {sort_stop_num} AS "Stop Number",
                                arrival_or_departure AS "Arrival or Departure",
                                CAST(AVG(timedelta_from_sched) AS integer) AS "Average Delay",
                                STDDEV_POP(timedelta_from_sched) AS "Standard Deviation"
                            FROM
                                stops_joined
                            WHERE
                                train_num = '{train_num}' AND
                                origin_year BETWEEN {year_range[0]} AND {year_range[1]}
                            GROUP BY station_code, {sort_stop_num}, arrival_or_departure
                            ORDER BY {sort_stop_num} ASC, arrival_or_departure ASC;
                            """
        single_trip_df = connect_and_query(single_trip_query)
        historical_df = connect_and_query(historical_query)
        print(single_trip_df)
        print(historical_df)
    else:
        raise dash.exceptions.PreventUpdate


######################
# NAVIGATION
######################
nav = dbc.Nav(
    [
        dbc.NavItem(dbc.NavLink("About the Project", active="exact", href="/data1050-app-home")),
        dbc.NavItem(dbc.NavLink("On-Time Performance Visualizer", active="exact", href="/data1050-app-viz")),
        dbc.NavItem(dbc.NavLink("On-Time Performance Comparison", active="exact", href="/data1050-app-enhancement")),
        dbc.DropdownMenu(
            [
                dbc.DropdownMenuItem("Github Repository", href="https://github.com/elizabeth-c-chen/data1050-amtrak-on-time-analysis", id="button-link"),
                dbc.DropdownMenuItem("View Data Retrieval & Database Loading Notebook", href="https://nbviewer.jupyter.org/github/elizabeth-c-chen/data1050-amtrak-on-time-analysis/blob/master/EDA_ETL.ipynb", id="button-link")
            ],
            label="View Source Code",
            nav=True
        ),
    ],
    pills=True,
    horizontal='center',
    card=True
)


######################
# PAGE LAYOUTS
######################
SHOW_THIS_PAGE_ON_LOAD = "/data1050-app-viz"
unfinished = html.P("This page is coming soon!", style={'text-align': 'center', 'font-size': 50})


data1050_app_home_layout = dbc.Container(
    [
        html.H3(html.A(children="Portfolio of Elizabeth C. Chen", href='/')),
        html.H4(html.A("Amtrak Northeast Regional On-Time Performance Explorer", href=SHOW_THIS_PAGE_ON_LOAD)),
        html.H6("A DATA 1050 Final Project", style={'padding-top': '-10px', 'padding-bottom': '-10px'}),
        nav,
        dbc.Row(
            [
                dbc.Col(unfinished, md=12, lg=12)
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

data1050_app_viz_layout = dbc.Container(
    [
        html.H3(html.A(children="Portfolio of Elizabeth C. Chen", href='/')),
        html.H4(html.A("Amtrak Northeast Regional On-Time Performance Explorer", href='/data1050-app-home')),
        html.H6("A DATA 1050 Final Project", style={'padding-top': '-10px', 'padding-bottom': '-10px'}),
        nav,
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

data1050_app_enhancement_layout = dbc.Container(
    [
        html.H3(html.A(children="Portfolio of Elizabeth C. Chen", href='/')),
        html.H4(html.A("Amtrak Northeast Regional On-Time Performance Explorer", href='/data1050-app-analysis')),
        html.H6("A DATA 1050 Final Project", style={'padding-top': '-10px', 'padding-bottom': '-10px'}),
        nav,
        dbc.Row(
            [
                dbc.Col([specific_trip_controls], md=5, lg=4),
                dbc.Col(hypo_test_view_initial, md=7, lg=8)
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

error_page_layout = html.Div(
    children=[
        html.H3(children="Uh oh! You have reached a page that doesn't exist"),
        html.H6(html.A(children='Click here to return to the homepage.', href='/'))
    ],
    style={'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'}
)

@app.callback(
    dash.dependencies.Output('page-content', 'children'),
    [dash.dependencies.Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/':
        return data1050_app_home_layout
    elif pathname == '/data1050-app-home':
        return data1050_app_home_layout
    elif pathname == '/data1050-app-viz':
        return data1050_app_viz_layout
    elif pathname == '/data1050-app-enhancement':
        return data1050_app_enhancement_layout
    else:
        return error_page_layout


if __name__ == '__main__':
    app.run_server(port=8051, debug=True)
