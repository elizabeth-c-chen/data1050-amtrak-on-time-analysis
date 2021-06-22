import requests
import os
import pandas as pd
import numpy as np
from datetime import date, timedelta
import logging
from utils import setup_logger, update_table

#############################
# Set up logger
#############################
logger = logging.Logger(__name__)
setup_logger(logger, 'etl.log')

assert os.environ.get('VC_TOKEN') is not None, 'empty weather API token!'

locations_urlstring = [
    'Boston,MA', 'Providence,RI', 'Kingston,RI', 'Westerly,RI', 'Mystic,CT',
    'New%20London,CT', 'Old%20Saybrook,CT', 'New%20Haven,CT', 'Bridgeport,CT',
    'Stamford,CT', 'New%20Rochelle,NY', 'Manhattan,NY', 'Newark,NJ', 'Iselin,NJ',
    'Trenton,NJ', 'Philadelphia,PA', 'Wilmington,DE', 'Aberdeen,MD', 'Baltimore,MD',
    'Baltimore%20BWI%20Airport,MD', 'New%20Carrollton,MD', 'Washington,DC'
    ]

locations_filestring = [
    'Boston_MA', 'Providence_RI', 'Kingston_RI', 'Westerly_RI', 'Mystic_CT',
    'New_London_CT', 'Old_Saybrook_CT', 'New_Haven_CT', 'Bridgeport_CT',
    'Stamford_CT', 'New_Rochelle_NY', 'Manhattan_NY', 'Newark_NJ', 'Iselin_NJ',
    'Trenton_NJ', 'Philadelphia_PA', 'Wilmington_DE', 'Aberdeen_MD', 'Baltimore_MD',
    'Baltimore_BWI_Airport_MD', 'New_Carrollton_MD', 'Washington_DC'
    ]

yesterday = str(date.today()-timedelta(days=1))


insert_into_weather = """
                      INSERT INTO
                          weather_hourly (
                              location,
                              obs_datetime,
                              temperature,
                              precipitation,
                              cloud_cover,
                              weather_type
                      )
                      VALUES
                          (%s, %s, %s, %s, %s, %s)
                      ON CONFLICT DO NOTHING;
                      """


def retrieve_weather_data(start=yesterday, end=yesterday):
    """
    Function to retrieve data from Visual Crossing Weather API for dates in dates list.
    If no params are given, defaults to retrieving data for the previous day only.

    Input:
                start       formatted as 'YYYY-MM-DD'
                end         formatted as 'YYYY-MM-DD'
    Returns:
                successful_retrievals
                    list of (location, filepath) tuples indicating successfully created files
    Example:
                retrieve_weather_data(start='2021-04-09', end='2021-04-09') for April 9, 2021 data
    """
    URL_ROOT = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/'
    QUERY_TYPE = 'weatherdata/history?&aggregateHours=1'
    DATES = '&startDateTime={}T00:00:00&endDateTime={}T23:59:00'.format(start, end)
    EXTRA_PARAMS = '&collectStationContributions=true&unitGroup=us&contentType=csv'
    URL_BASE = URL_ROOT + QUERY_TYPE + DATES + EXTRA_PARAMS
    URL_KEY = '&key=' + os.environ.get('VC_TOKEN')
    successful_retrievals = []
    failed_retrievals = []
    for locname, filename in zip(locations_urlstring, locations_filestring):
        CSVstring = './data/weather_raw/{}_weather_data_{}_{}.csv'.format(filename, start, end)
        if not os.path.exists(CSVstring):
            URL_LOC = '&location=' + locname
            URL = URL_BASE + URL_LOC + URL_KEY
            response = requests.get(URL)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                failed_retrievals.append((CSVstring, str(e)))
                continue
            csv_bytes = response.content
            with open(CSVstring, 'w', newline='\n') as csvfile:
                csvfile.write(csv_bytes.decode())
                csvfile.close()
            successful_retrievals.append((filename, CSVstring))
        elif os.path.exists(CSVstring):
            failed_retrievals.append((CSVstring, 'Error: File Already Exists'))
            continue
    if len(failed_retrievals) > 0:
        print('Failed to retrieve data for the following filenames:')
        for filestring, error in failed_retrievals:
            print('        FILE:   {}'.format(filestring))
            print('        REASON: {}'.format(error))
    return successful_retrievals


def process_weather_data(files_to_process):
    """
    This function is set for processing current (2021) weather data which is being retrieved daily.
    It takes a start and end date, which both default to yesterday if no arguments are given, and
    processes all raw files from the specified dates to a subset of columns. It then concatenates
    the newly processed data and the previously processed data, and then saves the complete 2021
    data to a CSV (with same name as the previously processed 2021 full data).

    Input:
                list of (location, filepath) tuples to process and combine with previous data
    Returns:
                nothing (updates the yearly combined data CSV file on disk)
    Example:
            files_to_process = [
                ('Boston_MA', './data/weather_raw/Boston_MA_weather_data_2021-04-11_2021-04-11.csv')
            ]
            process_weather_data(files_to_process)
    """
    successful_processes = []
    for location, read_string in files_to_process:
        cols_list = ['Address', 'Date time', 'Temperature',
                     'Weather Type', 'Precipitation', 'Cloud Cover']
        full_weather = pd.read_csv(read_string, usecols=cols_list)
        full_weather['Address'] = full_weather['Address'].str.replace(',', ', ')
        full_weather = full_weather[['Address', 'Date time', 'Temperature', 'Precipitation', 'Cloud Cover', 'Weather Type']]
        drop_na_index = full_weather[['Temperature', 'Precipitation', 'Cloud Cover']].replace('', np.nan).dropna().index
        full_weather = full_weather.iloc[drop_na_index]
        frac_kept = drop_na_index.shape[0]/full_weather.shape[0]
        prev2021_filestring = './data/weather/{}_weather_subset_2021.csv'.format(location)
        prev_weather = pd.read_csv(prev2021_filestring)
        combined_weather = pd.concat([prev_weather, full_weather], ignore_index=True, axis=0)
        combined_weather.drop_duplicates(inplace=True, ignore_index=True)
        combined_weather.to_csv(prev2021_filestring, index=False)
        successful_processes.append((prev2021_filestring, frac_kept))
    print('Successfully processed and combined the following raw data files with previous data:')
    for filestring, fraction in successful_processes:
        print('        FILE:          {}'.format(filestring))
        print('        FRACTION KEPT: {}'.format(fraction))


#######################################
# PostgreSQL ETL Function for Weather
#######################################
def ETL_previous_day_weather_data(conn):
    """
    Runs ETL for previous day's data
    """
    yesterday = str(date.today() - timedelta(days=1))
    URL_ROOT = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/'
    QUERY_TYPE = 'weatherdata/history?&aggregateHours=1'
    DATES = '&startDateTime={}T00:00:00&endDateTime={}T23:59:00'.format(yesterday, yesterday)
    EXTRA_PARAMS = '&collectStationContributions=true&unitGroup=us&contentType=csv'
    URL_BASE = URL_ROOT + QUERY_TYPE + DATES + EXTRA_PARAMS
    URL_KEY = '&key=' + os.environ.get('VC_TOKEN')
    successful_processes = []
    for location, filename in zip(locations_urlstring, locations_filestring):
        CSVstring = '/tmp/{}_weather_yesterday.csv'.format(filename)
        URL_LOC = '&location=' + location
        URL = URL_BASE + URL_LOC + URL_KEY
        response = requests.get(URL)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Error retrieving yesterday's weather data for {filename}: {e}")
            continue
        csv_bytes = response.content
        with open(CSVstring, 'w', newline='\n') as csvfile:
            csvfile.write(csv_bytes.decode())
            csvfile.close()
        full_weather = pd.read_csv(CSVstring, usecols=['Address', 'Date time', 'Temperature', 'Weather Type', 'Precipitation', 'Cloud Cover'])
        full_weather['Address'] = full_weather['Address'].str.replace(',', ', ')
        full_weather = full_weather[['Address', 'Date time', 'Temperature', 'Precipitation', 'Cloud Cover', 'Weather Type']]
        drop_na_index = full_weather[['Temperature', 'Precipitation', 'Cloud Cover']].replace('', np.nan).dropna().index
        full_weather = full_weather.iloc[drop_na_index]
        rows_kept = drop_na_index.shape[0]
        CSVstring_proc = f"/tmp/{filename}_weather_subset_yesterday.csv"
        full_weather.to_csv(CSVstring_proc, line_terminator='\n', index=False)
        successful_processes.append((filename, CSVstring_proc, rows_kept))
    for filename, filestring, rows_kept in successful_processes:
        update_table(conn, insert_into_weather, filestring)
        logger.info(f"Successful ETL of yesterday's weather data for {filename} (# Rows Kept: {rows_kept}/24)")
