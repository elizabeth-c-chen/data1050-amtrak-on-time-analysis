import time
import requests
from datetime import date
import lxml.html as lh


def make_dict():
    """
    Creates dictionary to hold raw data sorted by arrival and direction, then by station.
    """
    dictionary = {'Arrive': { s: [] for s in ['NYP', 'BOS', 'WAS'] },
                  'Depart': { s: [] for s in ['BOS', 'WAS', 'NHV', 'NYP', 'PHL', 'BAL', 'PVD', 'WIL', 'BWI', 
                                              'NWK', 'BBY', 'RTE', 'TRE', 'STM', 'NCR', 'KIN', 'NLC'] } 
                 }
    return dictionary


def make_dict_from_cols(col_names):
    """
    Create dictionary from a list of column names
    """
    dictionary = { col_name: [] for col_name in col_names }
    return dictionary


def convert_train_nums_to_string(train_nums_list):
    """
    Give a list of train numbers, converts it to a string that can be used in a url.
    """
    output = '&train_num=' + str(train_nums_list[0])
    for train_num in train_nums_list[1:]:
        output += '%2C' + str(train_num)
    return output


def convert_dates_to_string(dt_start, dt_end):
    """
    Function to convert a date object to a url string.
    """
    start = '&date_start=' + str(dt_start.month) + '%2F' + str(dt_start.day) + '%2F' + str(dt_start.year)
    end = '&date_end=' + str(dt_end.month) + '%2F' + str(dt_end.day) + '%2F' + str(dt_end.year)
    return start + end


def make_request(url):
    """
    Given a url, request the data and return the page content or None if 
    retrieving data failed on the first try.
    """
    page = None
    try: 
        req = requests.get(url, timeout=7)
        req.raise_for_status()
        page = req.content
    except requests.exceptions.RequestException as e:
        print("An exception occurred: ", e)
    return page


def try_request(url, max_retries=5):
    """
    Given a url and max number of retries (int), attempt to get the data/
    page content until max number of retries has been reached.
    """
    page = None
    for i in range(max_retries):
        try: 
            req = requests.get(url, timeout=5)
            req.raise_for_status()
            page = req.content
        except requests.exceptions.RequestException as e:
            print("An exception occurred: ", e)
    if page is None:
        print("No data retrieved for {} {}".format())
        #error handling here:  #logger.error('reached maximum number of FAILED attempts to get data')
    return page


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
    URL_END_DP = '&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort=schDp&sort_dir=ASC&co=gt&limit_mins=&dfon=1'
    URL_END_AR = '&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort=schAr&sort_dir=ASC&co=gt&limit_mins=&dfon=1'
    dates_string = convert_dates_to_string(start_date, end_date)
    arrive = ['NYP']
    depart = ['NHV', 'NYP', 'PHL', 'BAL', 'PVD', 'WIL', 'BWI', 'NWK', 
              'BBY', 'RTE', 'TRE', 'STM', 'NCR', 'KIN', 'NLC'] 
    urls = {'Arrive': [], 'Depart': []}
    for trains_list in northbound_trains:
        trains_string = convert_train_nums_to_string(trains_list)
        URL_BASE = URL_BEGIN + trains_string + dates_string + '&station=' 
        for station in depart + ['WAS']:
            urls['Depart'].append( (station, URL_BASE + station + URL_END_DP) )
        for station in arrive + ['BOS']:
            urls['Arrive'].append( (station, URL_BASE + station + URL_END_AR))
    for trains_list in southbound_trains:
        trains_string = convert_train_nums_to_string(trains_list)
        URL_BASE = URL_BEGIN + trains_string + dates_string + '&station=' 
        for station in depart + ['BOS']:
            urls['Depart'].append( (station, URL_BASE + station + URL_END_DP) )
        for station in arrive + ['WAS']:
            urls['Arrive'].append( (station, URL_BASE + station + URL_END_AR) )   
    return urls    


def fetch_data_from_urls(urls):
    """
    For each url in the urls dictionary, fetch the data and save it to a dictionary for later access.
    """
    raw_data = make_dict()
    start_time = time.time()
    for station, url in urls['Depart']:
        raw_data['Depart'][station].append(make_request(url))
    for station, url in urls['Arrive']:
        raw_data['Arrive'][station].append(make_request(url))
    print('Retrieved data in {} seconds'.format(time.time() - start_time))
    return raw_data

