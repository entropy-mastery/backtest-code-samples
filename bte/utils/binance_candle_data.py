#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime
from datetime import timedelta
from time import sleep
from binance.spot import Spot


client = Spot()

TIMEDELTA_MAP = {
    "1m": timedelta(minutes=1),
    "3m": timedelta(minutes=3),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
    "6h": timedelta(hours=6),
    "8h": timedelta(hours=8),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
    "1w": timedelta(days=7),
}


def _get_timedelta_for_candle(i):
    return TIMEDELTA_MAP.get(i, None)


def _pull_data(from_date, 
               to_date,
               n_candles,
               c_size, 
               symbol):
    """
    Function that extracts candles data from the exchange API.
    It has technical limitation of max 1000 rows due to the API specifics.
    Please prefer the other, "clean", function for the analysis

    Parameters
    ----------
    from_date : str
        Starting date of the data to pull in format '%Y-%m-%d %H:%M:%S'
    to_date : str
        End date of the data to pull in format '%Y-%m-%d %H:%M:%S'
    n_candles : int
        amount of canled to pull
    c_size : str
        Candle size.
        Supported values:
            1 minute (1m)
            3 minutes (3m)
            5 minutes (5m)
            15 minutes (15m)
            30 minutes (30m)
            1 hour (1h)
            2 hours (2h)
            4 hours (4h)
            6 hours (6h)
            8 hours (8h)
            12 hours (12h)
            1 day (1d)
            3 days (3d)
            1 week (1w)
            1 month (1M)
    symbol : str
        Pair to get prices on. For example 'BTCUSDT', 'ETHBTC', etc.

    Returns
    -------
    pandas.DataFrame
        DaraFrame with candles data
    """
    fd = datetime.datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')
    fd = (fd - datetime.datetime(1970, 1, 1)).total_seconds()*1000
    fd = int(fd)
    
    df = client.klines(symbol=symbol, 
                       interval=c_size, 
                       startTime=fd, 
                       #endTime, 
                       limit=n_candles)
    df = pd.DataFrame(data=df, 
                      columns=["open_time", 
                               "open",
                               "high",
                               "low",
                               "close",
                               "volume",
                               "close_time",
                               "Quote_asset_volume",
                               "Number_of_trades",
                               "Taker_buy_base_asset_volume",
                               "Taker_buy_quote_asset_volume",
                               "Unused_field_ignore"])
    df = df.sort_values(by="close_time", ascending=True)
    df = df[["close_time", "open", "high", "low", "close", "volume"]]
    for i in ["open", "high", "low", "close", "volume"]:
        df[i] = df[i].astype(float)
    df = df.rename(columns={"close_time": "Timestamp",
                            "open": "Open",
                            "high": "High",
                            "low": "Low",
                            "close": "Close",
                            "volume": "Volume"})
    df['Timestamp'] = df['Timestamp']+1
    df['Timestamp'] = pd.to_datetime(df['Timestamp'],unit='ms',origin='unix') # convert timestamp to datetime
    df = df[df["Timestamp"] < datetime.datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S')]
    df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df


def extract_data(symbol,
                 from_date,
                 to_date,
                 c_size,
                 ):
    """
    Function that extracts candles data from the exchange API.
    
    Parameters
    ----------
    from_date : str
        Starting date of the data to pull in format '%Y-%m-%d %H:%M:%S'
    to_date : str
        End date of the data to pull in format '%Y-%m-%d %H:%M:%S'
    c_size : str
        Candle size.
        Supported values:
            1 minute (1m)
            3 minutes (3m)
            5 minutes (5m)
            15 minutes (15m)
            30 minutes (30m)
            1 hour (1h)
            2 hours (2h)
            4 hours (4h)
            6 hours (6h)
            8 hours (8h)
            12 hours (12h)
            1 day (1d)
            3 days (3d)
            1 week (1w)
            1 month (1M)
    symbol : str
        Pair to get prices on. For example 'BTCUSDT', 'ETHBTC', etc.

    Returns
    -------
    pandas.DataFrame
        DaraFrame with candles data
    """
    end_date = datetime.datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S')
    start_date = datetime.datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')
    time_window = end_date - start_date
    t_int = _get_timedelta_for_candle(c_size)
    n = int(time_window/t_int)
    n_candles = 1000
    steps = int(n/n_candles)+1
    start = 0
    dataframe_list = []
    for i in range(start, steps):
        fd = datetime.datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')
        fd = fd + i * t_int * n_candles
        fd = fd.strftime('%Y-%m-%d %H:%M:%S')
        for a in range(72):
            try:
                print(f"{datetime.datetime.now()}: {i}/{steps}, {fd}")
                df = _pull_data(from_date=fd, 
                                to_date=end_date.strftime('%Y-%m-%d %H:%M:%S'),
                                n_candles=n_candles, 
                                c_size=c_size, 
                                symbol=symbol)  
                dataframe_list.append(df)  
                break
            except:
                print(f"download attempt # {a} failed")
                sleep(5*60)
    df = pd.concat(dataframe_list)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], format='%Y-%m-%d %H:%M:%S')
    df = df[(df["Timestamp"] >= start_date) & (df["Timestamp"] <= end_date)]
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    return df
