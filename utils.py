import pandas as pd
import psycopg2
import os
import plotly
import sys
import logging

assert os.environ.get('DATABASE_URL') is not None, 'database URL is not set!'


# Logging setup for errors/warnings/issues
def setup_logger(logger, output_file):
    logger.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter('%(asctime)s [%(funcName)s]: %(message)s'))
    logger.addHandler(stdout_handler)

    file_handler = logging.FileHandler(output_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(funcName)s] %(message)s'))
    logger.addHandler(file_handler)


# Helper Functions for app.py
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


def get_precip_types(precip_selected):
    output = '('
    for precip in precip_selected:
        output += '\'' + precip + '\'' + ', '
    output = output[0:-2] + ')'
    return output


def get_sort_from_train_num(train_num):
    if int(train_num) % 2 == 0:
        return 'nb_stop_num'
    else:
        return 'sb_stop_num'


def get_sort_from_direction(direction):
    if direction == 'Northbound':
        return 'nb_stop_num'
    else:
        return 'sb_stop_num'


def get_continuous_color(intermed):
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
    SOURCE: https://stackoverflow.com/questions/62710057/access-color-from-plotly-color-scale
    """
    colors, scale = plotly.colors.convert_colors_to_same_type(plotly.colors.diverging.RdYlGn)
    colorscale = plotly.colors.make_colorscale(colors, scale=scale)
    if intermed <= 0:
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

    if direction == 'Northbound':
        color_group_key = 'NB Station Group'
        arrival_station = 'BOS'
    elif direction == 'Southbound':
        color_group_key = 'SB Station Group'
        arrival_station = 'WAS'
    station_column = geo_route[color_group_key]
    delays = query_df['Average Delay']
    colors_dict = {station: 'rgb(0,0,0)' for station in station_column.unique()}
    counts = query_df['Num Records']
    delays_return = pd.Series(index=query_df['Station'], name='Delay Minutes')
    counts_return = pd.Series(index=query_df['Station'], name='Num Records')
    for station in query_df['Station']:
        if station != arrival_station:
            arr_or_dep = 'Departure'
        elif station == arrival_station:
            arr_or_dep = 'Arrival'
        stn_cond = query_df['Station'] == station
        arrdep_cond = query_df['Arrival or Departure'] == arr_or_dep
        td_minutes = delays.loc[(stn_cond) & (arrdep_cond)].values[0]
        delays_return.loc[station] = td_minutes
        counts_return.loc[station] = counts.loc[(stn_cond) & arrdep_cond].values[0]
        upper_bound = 18
        if td_minutes <= 0:
            colors_dict[station] = get_continuous_color(1)
        else:
            val = (upper_bound - td_minutes) / upper_bound
            colors_dict[station] = get_continuous_color(val)
    return colors_dict, delays_return, counts_return, color_group_key
