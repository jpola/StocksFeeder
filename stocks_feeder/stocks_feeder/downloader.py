from pandas_datareader import data as pdr
import pandas as pd
import datetime as dt
from dateutil.parser import parse

import fix_yahoo_finance as fyf


def today_date():
    '''
    utils:
    get the datetime of today
    '''
    date=dt.datetime.now().date()
    date=pd.to_datetime(date)
    return date


def get_data(source, ticker, start_date='2014-01-01', end_date=today_date()):

    _available_sources = ['yahoo', 'google']

    _available_tickers = ['MSFT', 'AAPL', '^GSPC']

    if source not in _available_sources:
        raise ValueError('Source: [' + source + '] is not available try: ' + ' , '.join(available_sources))

    if ticker not in _available_tickers:
        raise ValueError('Ticker: [' + ticker + '] is not yet supported: try: ' + ' , '.join(available_tickers))

    try:
        if type(start_date) is str:
            start_date = parse(start_date)
        if type(end_date) is str and type(end_date) is not pd.Timestamp:
            end_date = parse(end_date)
    except ValueError as ve:
        print(ve)
        raise

    _source = source.lower()
    if _source == 'google':
        _panel_data = pdr.DataReader(ticker, _source, start_date, end_date)
    elif _source == 'yahoo':
        _panel_data = pdr.get_data_yahoo(ticker, start_date, end_date)
        # This is some additional column which is available from yahoo.
        _panel_data.drop('Adj Close', inplace=True, axis=1)
    else:
        raise ValueError("Data source is not handled at the moment")

    return _panel_data

def save_pckl(data, file):
    data.to_pickle(file)

def load_pckl(file):
    _data = pd.read_pickle(file)
    return _data