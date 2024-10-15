import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


def download_data(symbol, interval: str):
    """
    Download one year of 1-minute interval data from Yahoo Finance by iterating weekly.

    Args:
    - symbol (str): The stock symbol to download the data for.

    Returns:
    - pd.DataFrame: A DataFrame containing the 1-minute interval data for one year.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    # List to store the data chunks
    data_list = []

    # Download the data in weekly chunks
    current_end = end_date
    current_start = current_end - timedelta(days=7)

    while current_start > start_date:
        # Download data for the current week
        data_chunk = yf.download(tickers=symbol,
                                 start=current_start.strftime('%Y-%m-%d'),
                                 end=current_end.strftime('%Y-%m-%d'),
                                 interval=interval)
        if not data_chunk.empty:
            data_list.append(data_chunk)

        # Move the window back by one week
        current_end = current_start
        current_start = current_end - timedelta(days=7)

    # Concatenate all the data chunks
    if data_list:
        full_data = pd.concat(data_list)
    else:
        full_data = pd.DataFrame()

    # Reset the index for consistency
    full_data.reset_index(inplace=True)
    full_data["Timestamp"] = full_data["Datetime"]
    return full_data
