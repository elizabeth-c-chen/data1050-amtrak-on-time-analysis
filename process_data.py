import pandas as pd
import numpy as np


def get_col_names(arrive_or_depart):
    """
    This function returns the proper column names for the data depending on whether the
    data being processed is arrival or departure data
    """
    if arrive_or_depart == 'Arrive':
        return ['Train Num',  'Station', 'Direction', 'Origin Date', 'Origin Year',
                'Origin Quarter', 'Origin Month', 'Origin Day', 'Origin Week Day',
                'Full Sch Ar Date', 'Sch Ar Date', 'Sch Ar Day', 'Sch Ar Time',
                'Act Ar Time', 'Arrive Diff', 'Service Disruption', 'Cancellations']
    elif arrive_or_depart == 'Depart':
        return ['Train Num',  'Station', 'Direction', 'Origin Date', 'Origin Year',
                'Origin Quarter', 'Origin Month', 'Origin Day', 'Origin Week Day',
                'Full Sch Dp Date', 'Sch Dp Date', 'Sch Dp Day', 'Sch Dp Time',
                'Act Dp Time', 'Depart Diff', 'Service Disruption', 'Cancellations']


def get_key_names(arrive_or_depart):
    """
    This function returns the proper keys to create the column names depending on
    whether the data being processed is arrival or departure data.
    """
    if arrive_or_depart == 'Arrive':
        return {'Sch Full Date': 'Full Sch Ar Date', 'Sch Abbr': 'Sch Ar',
                'Act Abbr': 'Act Ar', 'Diff': 'Arrive Diff'}

    elif arrive_or_depart == 'Depart':
        return {'Sch Full Date': 'Full Sch Dp Date', 'Sch Abbr': 'Sch Dp',
                'Act Abbr': 'Act Dp', 'Diff': 'Depart Diff'}


def process_columns(df, arrive_or_depart):
    """
    This function takes an input of the initial data (a pandas data frame) and whether it is
    arrival or departure data. It takes each column of the initial data and does various
    operations to create the fully processed data frame.
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
    new_df['Origin Quarter'] = origin_date.dt.quarter
    new_df['Origin Month'] = origin_date.dt.month
    new_df['Origin Day'] = origin_date.dt.day
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
    diff = (df['Act Date'] - df['Sched Date']).dt.total_seconds()/60
    new_df[ad_keys['Diff']] = np.rint(diff).astype(int)
    new_df['Service Disruption'] = df['Service Disruption'].replace('SD', 1).replace('', 0)
    new_df['Cancellations'] = df['Cancellations'].replace('C', 1).replace('', 0)
    return new_df.replace('', np.nan).dropna()
