import time
import requests
import re
import lxml.html as lh
import pandas as pd
from datetime import date, timedelta


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
    URL_BEGIN = 'https://juckins.net/amtrak_status/archive/html/history.php?train_num='
    URL_END_DP = '&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort=schDp&sort_dir=ASC&co\
        =gt&limit_mins=&dfon=1'
    URL_END_AR = '&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort=schAr&sort_dir=ASC&co\
        =gt&limit_mins=&dfon=1'
    dates_string = convert_dates_to_string(start_date, end_date)
    arrive = ['NYP']
    depart = ['NHV', 'NYP', 'PHL', 'BAL', 'PVD', 'WIL', 'BWI', 'NWK',
              'BBY', 'RTE', 'TRE', 'STM', 'NCR', 'KIN', 'NLC']
    urls = {'Arrive': [], 'Depart': []}
    for trains_list in northbound_trains:
        trains_string = convert_train_nums_to_string(trains_list)
        URL_BASE = URL_BEGIN + trains_string + dates_string + '&station='
        for station in depart + ['WAS']:
            urls['Depart'].append((station, URL_BASE + station + URL_END_DP))
        for station in arrive + ['BOS']:
            urls['Arrive'].append((station, URL_BASE + station + URL_END_AR))
    for trains_list in southbound_trains:
        trains_string = convert_train_nums_to_string(trains_list)
        URL_BASE = URL_BEGIN + trains_string + dates_string + '&station='
        for station in depart + ['BOS']:
            urls['Depart'].append((station, URL_BASE + station + URL_END_DP))
        for station in arrive + ['WAS']:
            urls['Arrive'].append((station, URL_BASE + station + URL_END_AR))
    return urls


def make_request(url):
    """
    Given a url, request the data and return the page content or None if
    retrieving data failed on the first try.
    """
    page = None
    try:
        req = requests.get(url, timeout=20)
        page = req.content
    except requests.exceptions.RequestException as e:
        print("An exception occurred: ", e)
    return page


def fetch_data_from_urls(urls):
    """
    For each url in the urls dictionary, fetch the data and save it to a
    dictionary for later access.
    """
    raw_data = make_dict()
    start_time = time.time()
    for station, url in urls['Depart']:
        data = make_request(url)
        raw_data['Depart'][station].append(data)
    for station, url in urls['Arrive']:
        raw_data['Arrive'][station].append(make_request(url))
    print('Retrieved data in {} seconds'.format(time.time() - start_time))
    return raw_data


def get_data(start=date.today()-timedelta(days=1), end=date.today()):
    """
    Function to retrieve new data from the website for specified dates, or else skip to next step.
    """
    # If querying a long time period, it is better to use smaller groups of trains (more requests)
    # northbound = [[66, 82, 86, 88], [94, 132, 150], [160, 162, 164, 166], [168, 170, 172, 174]]
    # southbound = [[67, 83, 93, 95], [99, 135, 137, 139], [161, 163, 165,167],[171, 173, 175, 195]]
    # If only querying a few days, we can just do them all at once
    northbound = [[66, 82, 86, 88, 94, 132, 150, 160, 162, 164, 166, 168, 170, 172, 174]]
    southbound = [[67, 83, 93, 95, 99, 135, 137, 139, 161, 163, 165, 167, 171, 173, 175, 195]]
    # Function can be found in fetch_data.py. It constructs the proper URL to run the query
    urls = construct_urls(northbound, southbound, start, end)
    # The function that retrieves the raw data
    data = fetch_data_from_urls(urls)
    return data


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
    Assuming input contains a match , extract and return the numerical data from input.
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
