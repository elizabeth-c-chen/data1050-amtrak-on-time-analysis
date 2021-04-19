import time
import requests
import re
import lxml.html as lh
import pandas as pd
import numpy as np
from datetime import date, timedelta

# CURRENT STATUS:
#       * minimal error checking
# TO DO:
#       * implement necessary error checking


def make_dict():
    """
    Creates dictionary to hold raw data sorted by arrival and direction, then by station.
    """
    dictionary = {'Arrive': {s: [] for s in ['NYP', 'BOS', 'WAS']},
                  'Depart': {s: [] for s in ['BOS', 'WAS', 'NHV', 'NYP', 'PHL', 'BAL',
                                             'PVD', 'WIL', 'BWI', 'NWK', 'BBY', 'RTE',
                                             'TRE', 'STM', 'NCR', 'KIN', 'NLC']}}
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
    arrive = ['NYP']
    depart = ['NHV', 'NYP', 'PHL', 'BAL', 'PVD', 'WIL', 'BWI', 'NWK',
              'BBY', 'RTE', 'TRE', 'STM', 'NCR', 'KIN', 'NLC']
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
    except requests.exceptions.HTTPError as e:
        print("An error occurred while retrieving data for the following url:")
        print('        {}'.format(url))
        print("Error: {}".format(e))
    page = response.content
    return page


def retrieve_data(start=date.today()-timedelta(days=1), end=date.today()):
    """
    Function to retrieve new data from the website for specified dates. If not given input
    start and end dates, defaults to retrieving data for yesterday.
    """
    # If querying a long time period, it is better to use smaller groups of trains (more requests)
    # northbound = [[66, 82, 86, 88], [94, 132, 150], [160, 162, 164, 166], [168, 170, 172, 174]]
    # southbound = [[67, 83, 93, 95], [99, 135, 137, 139], [161, 163, 165,167],[171, 173, 175, 195]]
    # If only querying a few days, we can just do them all at once
    northbound = [[66, 82, 86, 88, 94, 132, 150, 160, 162, 164, 166, 168, 170, 172, 174]]
    southbound = [[67, 83, 93, 95, 99, 135, 137, 139, 161, 163, 165, 167, 171, 173, 175, 195]]
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
        print('Failed to retrieve data for the following filenames:')
        for station, url in failed_retrievals:
            print('        STATION:   {}'.format(station))
            print('        URL:   {}'.format(url))
    print('Complete in {} seconds'.format(time.time() - start_time))
    return raw_data


# Processing Data Functions
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
                print("No data for this period, or an error occurred", station, arrive_or_depart)
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


if __name__ == '__main__':
    start = date.today()-timedelta(days=1)
    end = date.today()
    raw_data = retrieve_data(start=start, end=end)
    if raw_data is not None:
        depart = raw_data_to_raw_df(raw_data, 'Depart')
        arrive = raw_data_to_raw_df(raw_data, 'Arrive')
        arrive_filestring = './data/trains/raw_arrive_{}_{}.csv'.format(str(start), str(end))
        depart_filestring = './data/trains/raw_depart_{}_{}.csv'.format(str(start), str(end))
        arrive.to_csv(arrive_filestring, line_terminator='\n', index=False)
        depart.to_csv(depart_filestring, line_terminator='\n', index=False)
        full_arrive = process_columns(arrive, 'Arrive')
        full_depart = process_columns(depart, 'Depart')
        arrive_filestring2021 = './data/trains/processed_arrive_2021.csv'
        depart_filestring2021 = './data/trains/processed_depart_2021.csv'
        prev_arrive2021 = pd.read_csv(arrive_filestring2021)
        prev_depart2021 = pd.read_csv(depart_filestring2021)
        new_arrive2021 = pd.concat([prev_arrive2021, full_arrive], ignore_index=True, axis=0)
        new_depart2021 = pd.concat([prev_depart2021, full_depart], ignore_index=True, axis=0)
        new_arrive2021.to_csv(arrive_filestring2021, line_terminator='\n', index=False)
        new_depart2021.to_csv(depart_filestring2021, line_terminator='\n', index=False)
        print('Successfully retrieved and processed data for DATE: {}'.format(start))
    else:
        print('Failed to retrieve data.')
