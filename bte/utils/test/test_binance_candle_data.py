#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from utils.binance_candle_data import _pull_data
from utils.binance_candle_data import extract_data

import pandas as pd
import datetime


def test__pull_data():
    from_date = '2023-01-01 00:00:00'
    to_date = '2023-04-01 02:15:00'
    n_candles = 10 
    c_size = '15m'
    
    dataframes = []
    for s in ["BTCUSDT", "ETHUSDT", "ETHBTC"]:
        df = _pull_data(from_date=from_date, 
                        to_date=to_date,
                        n_candles=n_candles, 
                        c_size=c_size, 
                        symbol=s)
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] == n_candles
        assert df.columns.tolist() == ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
        assert df.shape[0] == df.drop_duplicates().shape[0]
        for i in ['Open', 'High', 'Low', 'Close']:
            if "USDT" in s:
                assert df[i].min() > 1
        for i in df['Timestamp'].values:
            assert isinstance(i, str)
            assert i[-5:-3] in ["00", "15", "30", "45"]
            assert datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S')
        dataframes.append(df)
        print(df.describe().to_string())    
    for i in range(len(dataframes)-1):
        assert not dataframes[i].equals(dataframes[i+1])
    
    from_date = '2023-01-01 00:00:00'
    to_date = '2023-01-01 02:15:00'
    n_candles = 10 
    c_size = '15m'
    dataframes = []
    for s in ["BTCUSDT", "ETHUSDT", "ETHBTC"]:
        df = _pull_data(from_date=from_date, 
                        to_date=to_date,
                        n_candles=n_candles, 
                        c_size=c_size, 
                        symbol=s)
        assert pd.to_datetime(df["Timestamp"], format='%Y-%m-%d %H:%M:%S').min() >= datetime.datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')
        assert pd.to_datetime(df["Timestamp"], format='%Y-%m-%d %H:%M:%S').max() < datetime.datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S')


def test_extract_data():
    from_date = '2024-01-01 00:00:00'
    to_date = '2024-06-20 02:16:00'
    c_size = '15m'
    
    dataframes = []
    for s in ["BTCUSDT", "ETHUSDT", "ETHBTC"]:
        df = extract_data(from_date=from_date, 
                          to_date=to_date,
                          c_size=c_size, 
                          symbol=s)
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
        assert df.shape[0] == df.drop_duplicates().shape[0]
        for i in ['Open', 'High', 'Low', 'Close']:
            if "USDT" in s:
                assert df[i].min() > 1
        for i in df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').values:
            assert isinstance(i, str)
            assert i[-5:-3] in ["00", "15", "30", "45"]
            assert datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S')
        dataframes.append(df)
        print(df.describe().to_string())    
    for i in range(len(dataframes)-1):
        assert not dataframes[i].equals(dataframes[i+1])
