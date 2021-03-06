import time
import logging
import requests
import re
import lxml.html as lh
import pandas as pd
import numpy as np
from datetime import date, timedelta
from utils import setup_logger, update_trains

#############################
# Set up logger
#############################
logger = logging.Logger(__name__)
setup_logger(logger, 'etl.log')


#############################
# Helper functions
#############################
def make_dict():
    """
    Creates dictionary to hold raw data sorted by arrival and direction, then by station.
    """
    dictionary = {
        'Arrive': {s: [] for s in ['NYP', 'NHV', 'PHL', 'BOS', 'WAS']},
        'Depart': {s: [] for s in ['BOS', 'BBY', 'RTE', 'PVD', 'KIN',
                                   'WLY', 'MYS', 'NLC', 'OSB', 'NHV',
                                   'BRP', 'STM', 'NRO', 'NYP', 'NWK',
                                   'EWR', 'MET', 'TRE', 'PHL', 'WIL',
                                   'ABE', 'BAL', 'BWI', 'NCR', 'WAS']}
        }
    return dictionary


def convert_train_nums_to_string(train_nums_list):
    """
    Give a list of train numbers, converts it to a string that can be used in a url.
    """
    output = str(train_nums_list[0])
    for train_num in train_nums_list[1:]:
        output += '%2C' + str(train_num)
    return output


def convert_dates_to_string(dt_start, dt_end):
    """
    Function to convert a date object to a url string.
    """
    start = '&date_start=' + str(dt_start.month) + '%2F' + str(dt_start.day) + \
        '%2F' + str(dt_start.year)
    end = '&date_end=' + str(dt_end.month) + '%2F' + str(dt_end.day) + \
        '%2F' + str(dt_end.year)
    return start + end


def construct_urls(northbound_trains, southbound_trains, start_date, end_date):
    """
    Inputs: 2 lists of lists of train numbers, 2 dates
        - list of northbound train subset lists
        - list of southbound train subset lists
        - start date for fetching data
        - end date for fetching data
    Outputs: dictionary of urls based on arrivals and departures from select stations
    """
    URL_ROOT = 'https://juckins.net/amtrak_status/archive/html/history.php?train_num='
    DFS = '&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1'
    ARR = '&sort=schAr'
    DEP = '&sort=schDp'
    URL_END = '&sort_dir=ASC&co=gt&limit_mins=&dfon=1'
    DATES = convert_dates_to_string(start_date, end_date)
    arrive = ['NYP', 'NHV', 'PHL']
    depart = ['BBY', 'RTE', 'PVD', 'KIN', 'WLY', 'MYS', 'NLC', 'OSB', 'NHV', 'BRP', 'STM', 'NRO',
              'NYP', 'NWK', 'EWR', 'MET', 'TRE', 'PHL', 'WIL', 'ABE', 'BAL', 'BWI', 'NCR']
    urls = {'Arrive': [], 'Depart': []}
    for trains_list in northbound_trains:
        TRAINS = convert_train_nums_to_string(trains_list)
        for station in depart + ['WAS']:
            STATION = '&station=' + station
            URL = URL_ROOT + TRAINS + DATES + STATION + DFS + DEP + URL_END
            urls['Depart'].append((station, URL))
        for station in arrive + ['BOS']:
            STATION = '&station=' + station
            URL = URL_ROOT + TRAINS + DATES + STATION + DFS + ARR + URL_END
            urls['Arrive'].append((station, URL))
    for trains_list in southbound_trains:
        TRAINS = convert_train_nums_to_string(trains_list)
        for station in depart + ['BOS']:
            STATION = '&station=' + station
            URL = URL_ROOT + TRAINS + DATES + STATION + DFS + DEP + URL_END
            urls['Depart'].append((station, URL))
        for station in arrive + ['WAS']:
            STATION = '&station=' + station
            URL = URL_ROOT + TRAINS + DATES + STATION + DFS + ARR + URL_END
            urls['Arrive'].append((station, URL))
    return urls


def make_request(url):
    """
    Given a url, request the data and return the page content or None if
    retrieving data failed on the first try.
    """
    page = None
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        page = response.content
    except requests.exceptions.HTTPError as e:
        logger.info("An error occurred while retrieving data for the following url:")
        logger.info('        {}'.format(url))
        logger.info("        Error: {}".format(e))
    return page


def retrieve_data(start=date.today()-timedelta(days=1), end=date.today()):
    """
    Function to retrieve new data from the website for specified dates. If not given input
    start and end dates, defaults to retrieving data for yesterday.
    """
    # If querying a long time period, it is better to use smaller groups of trains (more requests)
    # northbound = [[66, 82, 86, 88, 94, 132, 96, 176, 178, 190, 194], [150, 160, 162, 164, 166, 168, 170, 172, 174]]
    # southbound = [[67, 83, 93, 95, 99, 135, 65, 149, 169, 177], [137, 139, 161, 163, 165, 167, 171, 173, 175, 195]]
    # If only querying a few days, we can just do them all at once
    northbound = [[66, 82, 86, 88, 94, 132, 96, 176, 178, 190, 194, 150, 160, 162, 164, 166, 168, 170, 172, 174]]
    southbound = [[67, 83, 93, 95, 99, 135, 65, 149, 169, 177, 137, 139, 161, 163, 165, 167, 171, 173, 175, 195]]
    # Function can be found in fetch_data.py. It constructs the proper URL to run the query
    urls = construct_urls(northbound, southbound, start, end)
    raw_data = make_dict()
    failed_retrievals = []
    start_time = time.time()
    for station, url in urls['Depart']:
        data = make_request(url)
        if data is not None:
            raw_data['Depart'][station].append(data)
        else:
            failed_retrievals.append((station, url))
    for station, url in urls['Arrive']:
        data = make_request(url)
        if data is not None:
            raw_data['Arrive'][station].append(data)
        else:
            failed_retrievals.append((station, url))
    if len(failed_retrievals) > 0:
        logger.info('Failed to retrieve train data for the following filenames:')
        for station, url in failed_retrievals:
            logger.info('        STATION:   {}'.format(station))
            logger.info('        URL:   {}'.format(url))
    logger.info(f"Train data retrieval complete in {time.time() - start_time} seconds")
    return raw_data


#############################
# Processing Data Functions
#############################

def get_direction(num):
    """
    Return direction of the train (odd = Southbound, even = Northbound).
    """
    if num % 2 == 0:
        return 'Northbound'
    else:
        return 'Southbound'


def get_num(re_match):
    """
    Assuming input contains a match, extract and return the numerical data from input.
    """
    num_match = re.search('(?P<num>[0-9]+)', re_match)
    return int(num_match.group('num'))


def make_dict_from_cols(col_names):
    """
    Create dictionary from a list of column names
    """
    dictionary = {col_name: [] for col_name in col_names}
    return dictionary


def get_html_col_names(raw_data, arrive_or_depart):
    """
    Using NYP (station with both arrival times and departure times),
    retrieve column names from the HTML table, located in the 2nd row.
    """
    data_list = raw_data[arrive_or_depart]['NYP']
    page_content = data_list[0]
    doc = lh.fromstring(page_content)
    tr_elements = doc.xpath('//tr')
    html_col_names = [entry.text_content().strip() for entry in tr_elements[1]]
    return html_col_names


def raw_data_to_raw_df(raw_data, arrive_or_depart):
    """
    Function to put the raw html data in a dataframe for ease of processing.
    """
    col_names = get_html_col_names(raw_data, arrive_or_depart)
    N = 7
    data_dict = make_dict_from_cols(['Direction', 'Station'] + col_names)
    for station in raw_data[arrive_or_depart].keys():
        data_list = raw_data[arrive_or_depart][station]
        L = len(data_list)
        for i in range(L):
            page_content = data_list[i]
            doc = lh.fromstring(page_content)
            tr_elements = doc.xpath('//tr')
            if len(tr_elements) > 3:
                title = tr_elements[0].text_content()
                direction = get_direction(get_num(title))
                for j in range(2, len(tr_elements)):
                    table_row = tr_elements[j]
                    if len(table_row) == N:
                        data_dict['Direction'].append(direction)
                        data_dict['Station'].append(station)
                        for col_name, entry in zip(col_names, table_row):
                            data = entry.text_content()
                            data_dict[col_name].append(data)
                    else:
                        continue
            else:
                logger.info(f"""STATION:   {station}  ({arrive_or_depart}) | No data for time period, or an error occurred during data retrieval.""")
    return pd.DataFrame.from_dict(data_dict)


def get_key_names(arrive_or_depart):
    """
    This function returns the proper keys to create the column names depending on
    whether the data being processed is arrival or departure data.
    """
    if arrive_or_depart == 'Arrive':
        return {'Sch Full Date': 'Full Sch Ar Date', 'Sch Abbr': 'Sch Ar',
                'Act Full Date': 'Full Act Ar Date', 'Act Abbr': 'Act Ar', 'Diff': 'Arrive Diff'}

    elif arrive_or_depart == 'Depart':
        return {'Sch Full Date': 'Full Sch Dp Date', 'Sch Abbr': 'Sch Dp',
                'Act Full Date': 'Full Act Dp Date', 'Act Abbr': 'Act Dp', 'Diff': 'Depart Diff'}


def process_columns(df, arrive_or_depart):
    """
    This function takes an input of the initial data (a pandas data frame) and whether it is
    arrival or departure data. It takes each column of the initial data and does various
    operations to create the fully processed data frame.
    Inputs:
            df - pandas dataframe of raw data
            arrive_or_depart - string 'Arrive' or 'Depart' to indicate which dict keys to use
    """
    # The specific keys depending on if new_df is for arr or dep data
    ad_keys = get_key_names(arrive_or_depart)
    new_df = pd.DataFrame()
    new_df['Train Num'] = pd.to_numeric(df['Train #'])
    new_df['Station'] = df['Station']
    new_df['Direction'] = df['Direction']

    origin_date = pd.to_datetime(df['Origin Date'], format="%m/%d/%Y", exact=False, errors='coerce')
    new_df['Origin Date'] = origin_date
    new_df['Origin Year'] = origin_date.dt.year
    new_df['Origin Month'] = origin_date.dt.month
    new_df['Origin Week Day'] = origin_date.dt.day_name()

    sched_full_date = pd.to_datetime(df[ad_keys['Sch Abbr']],
                                     format='%m/%d/%Y %I:%M %p',
                                     exact=False, errors='coerce')
    new_df[ad_keys['Sch Full Date']] = sched_full_date
    new_df[ad_keys['Sch Abbr'] + ' Date'] = sched_full_date.dt.date
    new_df[ad_keys['Sch Abbr'] + ' Day'] = sched_full_date.dt.day_name()
    new_df[ad_keys['Sch Abbr'] + ' Time'] = sched_full_date.dt.time
    act_time = pd.to_datetime(df[ad_keys['Act Abbr']], format='%I:%M%p',
                              exact=False, errors='coerce')
    new_df[ad_keys['Act Abbr'] + ' Time'] = act_time.dt.time

    df['Sched Date'] = sched_full_date
    df['Act Date'] = pd.to_datetime(sched_full_date.dt.date.astype(str) + " " +
                                    df[ad_keys['Act Abbr']].astype(str),
                                    exact=False, errors='coerce')
    max_expected_delay = pd.Timedelta(hours=10)
    delta = df['Act Date'] - df['Sched Date']
    m_late = (delta < max_expected_delay) & (-1*max_expected_delay > delta)
    m_early = (-1*delta < max_expected_delay) & (-1*max_expected_delay > -1*delta)
    df.loc[m_late, 'Act Date'] += pd.Timedelta(days=1)
    df.loc[m_early, 'Act Date'] -= pd.Timedelta(days=1)
    new_df[ad_keys['Act Full Date']] = df['Act Date']
    diff = (df['Act Date'] - df['Sched Date']).dt.total_seconds()/60
    new_df[ad_keys['Diff']] = np.rint(diff).astype(int)
    df['Service Disruption'] = df['Service Disruption'].replace('SD', 1).replace('', 0)
    df['Cancellations'] = df['Cancellations'].replace('C', 1).replace('', 0)
    new_df['Service Disruption'] = df['Service Disruption'].astype(int)
    new_df['Cancellations'] = df['Cancellations'].astype(int)
    return new_df.replace('', np.nan).dropna()


#######################################
# PostgreSQL insert into stops command
#######################################
insert_into_stops = """
                    INSERT INTO
                        stops (
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
                            cancellations
                          )
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """


#######################################
# PostgreSQL ETL Function for Trains
#######################################
def ETL_previous_day_train_data(conn):
    """
    Runs ETL for previous day's data
    """
    yesterday = date.today()-timedelta(days=1)
    raw_data = retrieve_data(start=yesterday, end=yesterday)
    depart = raw_data_to_raw_df(raw_data, 'Depart')
    arrive = raw_data_to_raw_df(raw_data, 'Arrive')
    arrive.to_csv('/tmp/arrive_yesterday_raw.csv', line_terminator='\n', index=False)
    depart.to_csv('/tmp/depart_yesterday_raw.csv', line_terminator='\n', index=False)
    full_arrive = process_columns(arrive, 'Arrive')
    full_depart = process_columns(depart, 'Depart')
    full_arrive.to_csv('/tmp/arrive_yesterday.csv', line_terminator='\n', index=False)
    full_depart.to_csv('/tmp/depart_yesterday.csv', line_terminator='\n', index=False)
    update_trains(conn, insert_into_stops, 'Arrival', '/tmp/arrive_yesterday.csv')
    logger.info(f"Successful ETL of yesterday's arrival data (# Rows Kept: {full_arrive.shape[0]}/{arrive.shape[0]})")
    update_trains(conn, insert_into_stops, 'Departure', '/tmp/depart_yesterday.csv')
    logger.info(f"Successful ETL of yesterday's departure data (# Rows Kept: {full_depart.shape[0]}/{depart.shape[0]})")
