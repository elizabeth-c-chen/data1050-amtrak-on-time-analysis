import requests
import os
import pandas as pd
import numpy as np
from datetime import date, timedelta

assert os.environ.get('VC_TOKEN') is not None, 'empty weather API token!'

locations = ['Boston,MA', 'Providence,RI', 'Kingston,RI', 'New%20London,CT',
             'New%20Haven,CT', 'Stamford,CT', 'Manhattan,NY', 'Newark,NJ',
             'Trenton,NJ', 'Philadelphia,PA', 'Wilmington,DE', 'Baltimore,MD',
             'Baltimore%20BWI%20Airport,MD', 'New%20Carrollton,MD', 'Washington,DC']

location_names_for_files = ['Boston_MA', 'Providence_RI', 'Kingston_RI', 'New_London_CT',
                            'New_Haven_CT', 'Stamford_CT', 'Manhattan_NY', 'Newark_NJ',
                            'Trenton_NJ', 'Philadelphia_PA', 'Wilmington_DE', 'Baltimore_MD',
                            'Baltimore_BWI_Airport_MD', 'New_Carrollton_MD', 'Washington_DC']

yesterday = str(date.today()-timedelta(days=1))


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
    DATES = '&startDateTime={}T00:00:00&endDateTime={}T23:59:00&unitGroup'.format(start, end)
    URL_BASE = URL_ROOT + QUERY_TYPE + DATES
    URL_KEY = '&key=' + os.environ.get('VC_TOKEN')
    successful_retrievals = []
    failed_retrievals = []
    for locname, filename in zip(locations, location_names_for_files):
        print('Retrieving data for LOCATION: {}'.format(filename))
        print('    and DATE RANGE: {}T00:00:00 to {}T23:59:00'.format(start, end))
        CSVstring = './data/weather_original/{}_weather_data_{}_{}.csv'.format(filename, start, end)
        if not os.path.exists(CSVstring):
            URL_LOC = '=us&contentType=csv&location=' + locname
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
    if len(successful_retrievals) > 0:
        print('Successfully collected data has been saved at the following filenames:')
        for location, filestring in successful_retrievals:
            print('        FILE:   {}'.format(filestring))
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
                (
                    'Boston_MA',
                    './data/weather_original/Boston_MA_weather_data_2021-04-11_2021-04-11.csv'
                )
            ]
            process_weather_data(files_to_process)
    """
    successful_processes = []
    for location, CSVstring in files_to_process:
        cols_list = ['Address', 'Date time', 'Latitude', 'Longitude', 'Temperature',
                     'Precipitation', 'Cloud Cover', 'Conditions']
        full_weather = pd.read_csv(CSVstring, usecols=cols_list)
        full_weather['Address'] = full_weather['Address'].str.replace(',', ', ')
        dropna_weather = full_weather.replace('', np.nan).dropna()
        frac_kept = dropna_weather.shape[0]/full_weather.shape[0]
        cond_cols = dropna_weather['Conditions'].str.split(', ', expand=True)
        precip_marker = cond_cols[0].loc[cond_cols[0].isin(['Rain', 'Snow'])]
        not_precip_marker = cond_cols[0].loc[cond_cols.index.difference(precip_marker.index)]
        precip_column = pd.Series(index=dropna_weather.index, dtype='object')
        precip_column.iloc[precip_marker.index] = precip_marker.values
        precip_column.iloc[not_precip_marker.index] = 'No Precipitation'
        dropna_weather['Precipitation Type'] = precip_column
        prev_2021_CSVstring = './data/weather/{}_weather_2021_subset.csv'.format(location)
        prev_weather = pd.read_csv(prev_2021_CSVstring)
        combined_weather = pd.concat([prev_weather, dropna_weather], ignore_index=True, axis=0)
        combined_weather.drop_duplicates(inplace=True, ignore_index=True)
        combined_weather.to_csv(prev_2021_CSVstring, index=False)
        successful_processes.append((CSVstring, frac_kept))
    print('Successfully processed and combined the following raw data files with previous data:')
    for filestring, fraction in successful_processes:
        print('        FILE:          {}'.format(filestring))
        print('        FRACTION KEPT: {}'.format(fraction))


if __name__ == '__main__':
    # For quick and easy data retrieval on a daily basis, this is set up so that it will just
    # retieve yesterdays data when called from the command line.
    successful_retrievals = retrieve_weather_data()
    if len(successful_retrievals) > 0:
        process_weather_data(files_to_process=successful_retrievals)
