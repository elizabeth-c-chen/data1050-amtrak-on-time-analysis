import os
import pandas as pd
import psycopg2
import requests
import logging
from datetime import date, timedelta, datetime

from trains_retrieve_and_process_data import retrieve_data, raw_data_to_raw_df, process_columns
from database_ETL_functions import update_trains, update_table,\
     insert_into_stops_command, insert_into_weather_command

from utils import setup_logger

logger = logging.Logger(__name__)
setup_logger(logger, 'etl.log')

conn = psycopg2.connect(os.environ.get('DATABASE_URL'), sslmode='require')

assert conn is not None, 'need to fix conn!!'
assert os.environ.get('VC_TOKEN') is not None , 'empty token!'

yesterday = str(date.today()-timedelta(days=1))

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
    'Trenton_NJ', 'Philadelphia_PA', 'Wilmington_DE','Aberdeen_MD', 'Baltimore_MD',
    'Baltimore_BWI_Airport_MD', 'New_Carrollton_MD', 'Washington_DC'
]


def ETL_previous_day_train_data(yesterday):
    raw_data = retrieve_data(start=yesterday, end=yesterday)
    depart = raw_data_to_raw_df(raw_data, 'Depart')
    arrive = raw_data_to_raw_df(raw_data, 'Arrive')
    arrive.to_csv('./temp/arrive_yesterday_raw.csv', line_terminator='\n', index=False)
    depart.to_csv('./temp/depart_yesterday_raw.csv', line_terminator='\n', index=False)
    full_arrive = process_columns(arrive, 'Arrive')
    full_depart = process_columns(depart, 'Depart')
    full_arrive.to_csv('./temp/arrive_yesterday.csv', line_terminator='\n', index=False)
    full_depart.to_csv('./temp/depart_yesterday.csv', line_terminator='\n', index=False)
    update_trains(conn, insert_into_stops_command(), 'Arrival', './temp/arrive_yesterday.csv')
    time_now = datetime.now()
    logger.info(f"{time_now}: Successful ETL of yesterday's arrival data for (# Rows Kept: {full_arrive.shape[0]}/{arrive.shape[0]})")
    update_trains(conn, insert_into_stops_command(), 'Departure', './temp/depart_yesterday.csv')
    time_now = datetime.now()
    logger.info(f"{time_now}: Successful ETL of yesterday's departure data for (# Rows Kept: {full_depart.shape[0]}/{depart.shape[0]})")


def ETL_previous_day_weather_data(yesterday):
    URL_ROOT = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/'
    QUERY_TYPE = 'weatherdata/history?&aggregateHours=1'
    DATES = '&startDateTime={}T00:00:00&endDateTime={}T23:59:00'.format(yesterday, yesterday)
    EXTRA_PARAMS = '&collectStationContributions=true&unitGroup=us&contentType=csv'
    URL_BASE = URL_ROOT + QUERY_TYPE + DATES + EXTRA_PARAMS
    URL_KEY = '&key=' + os.environ.get('VC_TOKEN')
    successful_processes = []
    for location, filename in zip(locations_urlstring, locations_filestring):
        CSVstring = './temp/{}_weather_yesterday.csv'.format(filename)
        URL_LOC = '&location=' + location
        URL = URL_BASE + URL_LOC + URL_KEY
        response = requests.get(URL)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            time_now = datetime.now()
            logger.warning(f"{time_now}: Error retrieving yesterday's weather data for {filename}: {e}")
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
        CSVstring_proc = f"./temp/{filename}_weather_subset_yesterday.csv"
        full_weather.to_csv(CSVstring_proc, line_terminator='\n', index=False)
        successful_processes.append((filename, CSVstring_proc, rows_kept))
    for filename, filestring, rows_kept in successful_processes:
        update_table(conn, insert_into_weather_command(), filestring)
        time_now = datetime.now()
        logger.info(f"{time_now}: Successful ETL of yesterday's weather data for {filename} (# Rows Kept: {rows_kept}/24)")


if __name__ == '__main__':
    ETL_previous_day_train_data(yesterday)
    ETL_previous_day_weather_data(yesterday)
