import os
import sys
import csv
import psycopg2
import pandas as pd
import plotly
import logging
import datetime


assert os.environ.get('DATABASE_URL') is not None, 'database URL is not set!'


#############################
# Set up logger
#############################
def setup_logger(logger, output_file):
    logger.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter('%(asctime)s [%(funcName)s]: %(message)s'))
    logger.addHandler(stdout_handler)

    file_handler = logging.FileHandler(output_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(funcName)s] %(message)s'))
    logger.addHandler(file_handler)


logger = logging.Logger(__name__)
setup_logger(logger, 'etl.log')

#############################
# Database commands setup
#############################
join_command = """
            INSERT INTO stops_joined (
            SELECT
                stop_id,
                arrival_or_departure,
                train_num,
                station_code,
                direction,
                origin_date,
                origin_year,
                origin_month,
                origin_week_day,
                full_sched_arr_dep_datetime,
                sched_arr_dep_date,
                sched_arr_dep_week_day,
                sched_arr_dep_time,
                act_arr_dep_time,
                full_act_arr_dep_datetime,
                timedelta_from_sched,
                service_disruption,
                cancellations,
                crew_change,
                nb_stop_num,
                sb_stop_num,
                temperature,
                precipitation,
                cloud_cover,
                weather_type
            FROM
                stops s
                INNER JOIN (
                    SELECT
                        station_code AS si_station_code,
                        amtrak_station_name,
                        crew_change,
                        weather_location_name,
                        nb_stop_num,
                        sb_stop_num
                    FROM
                        station_info) si ON s.station_code = si.si_station_code
                INNER JOIN weather_hourly wh ON wh.location = si.weather_location_name
                    AND DATE_TRUNC('hour', s.full_act_arr_dep_datetime) = wh.obs_datetime
            ORDER BY
                s.full_sched_arr_dep_datetime
            );
            TRUNCATE TABLE stops;
            TRUNCATE TABLE weather_hourly;
            """

remove_duplicates = """
                    DELETE
                    FROM stops_joined
                    WHERE stops_joined.stop_id IN
                    (
                        SELECT sj_stop_id
                        FROM(
                            SELECT
                                *,
                                sj.stop_id AS sj_stop_id,
                                row_number() OVER (
                                    PARTITION BY
                                        origin_date,
                                        train_num,
                                        station_code,
                                        arrival_or_departure
                                    ORDER BY stop_id
                                )
                            FROM stops_joined sj
                        ) s
                        WHERE row_number >= 2
                    );
                    """


update_precip = """
                UPDATE
                    stops_joined
                SET
                    precip_type = (
                        CASE WHEN weather_type = '' AND precipitation = 0 THEN
                            'None'
                        WHEN weather_type LIKE '%Snow%'
                            AND weather_type LIKE '%Rain%' THEN
                            'Snow'
                        WHEN weather_type LIKE '%Snow%'
                            AND weather_type NOT LIKE '%Rain%' THEN
                            'Snow'
                        WHEN weather_type LIKE '%Rain%'
                            AND weather_type NOT LIKE '%Snow%' THEN
                            'Rain'
                        WHEN weather_type = '' AND precipitation > 0 AND temperature >= 32 THEN
                            'Rain'
                        WHEN weather_type = '' AND precipitation > 0 AND temperature < 32 THEN
                            'Snow'
                        END)
                WHERE
                    weather_type IS NOT NULL;
                """


insert_into_logs = """
                   INSERT INTO 
                       query_logs (
                           submit_datetime,
                           sql_content
                       )
                   VALUES
                        (%s, %s)
                   ON CONFLICT DO NOTHING;
                   """


##############################
# Database operation functions
##############################
def execute_command(conn, command):
    """
    Execute specified command in PostgreSQL database.
    """
    try:
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(f"""DATABASE ERROR: {error}""")
        conn.rollback()


def update_table(conn, command, csv_file):
    """
    Insert rows from a CSV file into table specified by the command.
    """
    cur = conn.cursor()
    with open(csv_file, newline='') as file:
        info_reader = csv.reader(file, delimiter=',')
        next(info_reader)  # Skip header
        for row in info_reader:
            try:
                cur.execute(command, tuple(row))
            except (Exception, psycopg2.DatabaseError) as error:
                logger.info(f"""DATABASE ERROR: {error}""")
                conn.rollback()
        conn.commit()


def update_trains(conn, command, arr_or_dep, csv_file):
    """
    Insert rows from trains CSV file into table specified by the command.
    """
    cur = conn.cursor()
    with open(csv_file, newline='') as file:
        info_reader = csv.reader(file, delimiter=',')
        next(info_reader)
        for row in info_reader:
            try:
                cur.execute(command, tuple([arr_or_dep] + row))
            except (Exception, psycopg2.DatabaseError) as error:
                logger.info(f"""DATABASE ERROR: {error}""")
                conn.rollback()
        conn.commit()

def update_logs(conn, user_query):
    """
    Insert user's query into the logs table.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            insert_into_logs,
            tuple([datetime.datetime.now(datetime.timezone.utc), user_query])
        )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()


def join_datasets(conn):
    """
    Join stops and weather tables and update the precipitation column.
    """
    execute_command(conn, join_command)
    execute_command(conn, remove_duplicates)
    execute_command(conn, update_precip)
    logger.info("Successful join of new stops and weather data.")


def connect_and_query(query, is_primary=False):
    """
    Connect to the PostgreSQL database and submit query and return the results.
    """
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'), sslmode='require')
    if is_primary:
        update_logs(conn, query)
    query_data = pd.read_sql(query, conn)
    conn.close()
    return query_data


#####################################
# Functions to help construct queries
#####################################
def get_days(days_selected):
    """
    Return the string used for PostgreSQL days of week query.
    """
    output = '('
    day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday',
                 'Thursday', 'Friday', 'Saturday']
    for i in range(len(day_names)):
        if i in days_selected:
            output += '\'' + day_names[i] + '\'' + ', '
    output = output[0:-2] + ')'
    return output


def get_precip_types(precip_selected):
    """
    Return the string used for PostgreSQL precipitation type query.
    """
    output = '('
    for precip in precip_selected:
        output += '\'' + precip + '\'' + ', '
    output = output[0:-2] + ')'
    return output


def get_sort_from_direction(direction):
    """
    Calculate direction from even/oddness of train_num.
    """
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
    colorscheme = plotly.colors.diverging.RdYlGn
    colors, scale = plotly.colors.convert_colors_to_same_type(colorscheme)
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


def get_default_colors(stations_list, color):
    color_dict = {station_code: color for station_code in stations_list}
    return color_dict


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
    colors_dict = {station: 'rgb(0, 30, 105)' for station in station_column.unique()}  # default color
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
        upper_bound = 20
        val = (upper_bound - td_minutes) / upper_bound
        colors_dict[station] = get_continuous_color(val)
    return colors_dict, delays_return, counts_return, color_group_key
