import pandas as pd
import psycopg2
import os
import plotly

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


def get_colors(geo_route, query_df):
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
