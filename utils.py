import pandas as pd
import psycopg2
import os
import plotly
from dash.dependencies import Input, Output, State

assert os.environ.get('DATABASE_URL') is not None, 'database URL is not set!'

# Helper Functions
def connect_and_query(query):
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'), sslmode='require')
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
def get_seasons(seasons_selected):
    output = '('
    for season in seasons_selected:
        output += '\'' + season + '\'' + ', '
    output = output[0:-2] + ')'
    return f"t.season IN {output}"

def get_extreme_temp(temp_selected):
    output = '('
    for temp in temp_selected:
        output += '\'' + temp + '\'' + ', '
    output = output[0:-2] + ')'
    return f"t.extreme_temperature IN {output}"

def get_cloud_levels(levels_selected):
    output = '('
    for level in levels_selected:
        output += '\'' + level + '\'' + ', '
    output = output[0:-2] + ')'
    return f"t.cloud_level IN {output}"

def get_precip_types(precip_selected):
    output = '('
    for precip in precip_selected:
        output += '\'' + precip + '\'' + ', '
    output = output[0:-2] + ')'
    return f"t.precip_type IN {output}"

def get_precip_levels(levels_selected):
    output = '('
    for level in range(levels_selected) + 1:
        output += '\'' + level + '\'' + ', '
    output = output[0:-2] + ')'
    return f"t.precip_level IN {output}"

def get_query_string(selected_items):
    if selected_items[0] in {'Fall', 'Winter', 'Spring', 'Summer'}:
        return get_seasons(selected_items)
    elif selected_items[0] in {'hot', 'cold', 'between'}:
        return get_extreme_temp(selected_items)
    elif selected_items[0] in {'clear', 'partly cloudy', 'mostly cloudy', 'overcast'}:
        return get_cloud_levels(selected_items)
    elif selected_items[0] in {'light', 'moderate', 'heavy', 'none'}:
        return get_precip_levels(selected_items)
    elif selected_items[0] in {'snow', 'rain', 'other', 'no pcp'}:
        return get_precip_types(selected_items)

def set_submit_callback_states_list(n_clicks):
    if n_clicks is not None:
        submit_callback_states_list = [
            State('direction-selector', 'value'),
            State('days-of-week-checkboxes', 'value'),
            State('sd-selector', 'value'),
            State('cancellation-selector', 'value'),
            State('chosen-option', 'value')
        ]
    else:
        submit_callback_states_list = [
            State('direction-selector', 'value'),
            State('days-of-week-checkboxes', 'value'),
            State('sd-selector', 'value'),
            State('cancellation-selector', 'value')
        ]
    return submit_callback_states_list


submit_callback_states_list = set_submit_callback_states_list(0)

def get_submit_callback_states_list():
    return  submit_callback_states_list

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

def get_colors(geo_route, query_df):
    """
    Create the path color groups according to data from query.
    """
    direction = query_df['Direction'].iloc[0]
    colors, scale = plotly.colors.convert_colors_to_same_type(plotly.colors.sequential.Turbo)
    colorscale = plotly.colors.make_colorscale(colors, scale=scale)
    if direction == 'Northbound':
        color_group_key = 'NB Station Group'
        arrival_station = 'BOS'
    elif direction == 'Southbound':
        color_group_key = 'SB Station Group'
        arrival_station = 'WAS'
    station_column = geo_route[color_group_key]
    delays = query_df['Average Delay']
    colors_dict = {station: 'rgb(0,0,0)' for station in station_column.unique()}
    for station in query_df['Station']:
        if station != arrival_station:
            arr_or_dep = 'Departure'
        elif station == arrival_station:
            arr_or_dep = 'Arrival'
        td_minutes = delays.loc[(query_df['Station'] == station)  & (query_df['Arrival or Departure'] == arr_or_dep) ].values[0]
        # Change these numbers later
        if td_minutes <= 0:
            colors_dict[station] = get_continuous_color(colorscale, 0.15)
        elif td_minutes > 0 and td_minutes <= 5:
            colors_dict[station] = get_continuous_color(colorscale, 0.4)
        elif td_minutes > 5 and td_minutes <= 8:
            colors_dict[station] = get_continuous_color(colorscale, 0.55)
        elif td_minutes > 8 and td_minutes <= 12:
            colors_dict[station] = get_continuous_color(colorscale, 0.67)
        elif td_minutes > 12 and td_minutes <= 16:
            colors_dict[station] = get_continuous_color(colorscale, 0.8)
        elif td_minutes > 16 and td_minutes <= 20:
            colors_dict[station] = get_continuous_color(colorscale, 0.9)
        elif td_minutes > 20: 
            colors_dict[station] = get_continuous_color(colorscale, 1)
    return colors_dict, delays, color_group_key