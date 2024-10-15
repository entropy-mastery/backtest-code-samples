import datetime
import pandas as pd
import copy


def rolling_forward_max_min(df, window):
    rolling_max_high = []
    rolling_min_low = []
    rolling_max_close = []
    rolling_min_close = []
    rolling_mean_close = []
    
    for _, current_time in enumerate(df.index):
        # Define the time window: rows with Timestamp > current row and <= current row + window
        window_end = current_time + window
        df_window = df[(df.index > current_time) & (df.index <= window_end)]
        
        # Get max and min values of 'High' and 'Low' in this window
        if not df_window.empty:
            rolling_max_high.append(df_window['High'].max())
            rolling_min_low.append(df_window['Low'].min())
            rolling_max_close.append(df_window['Close'].max())
            rolling_min_close.append(df_window['Close'].min())
            rolling_mean_close.append(df_window['Close'].mean())
        else:
            rolling_max_high.append(None)
            rolling_min_low.append(None)
            rolling_max_close.append(None)
            rolling_min_close.append(None)
            rolling_mean_close.append(None)
    
    # Add these values as new columns to the original dataframe
    df['Rolling_Max_High'] = rolling_max_high
    df['Rolling_Min_Low'] = rolling_min_low
    df['Rolling_Max_Close'] = rolling_max_close
    df['Rolling_Min_Close'] = rolling_min_close
    df['Rolling_Mean_Close'] = rolling_mean_close

    return df


def _get_data_for_analysis(df_candles, hp):
    df = copy.deepcopy(df_candles)
    if isinstance(df['Timestamp'].iloc[0], int):
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df.set_index('Timestamp', inplace=True)
    df = rolling_forward_max_min(df, hp)

    for start_price in ["Close", 
                        "Low", 
                        "High"]:
        for change_price in ['Rolling_Max_High', 
                            'Rolling_Min_Low', 
                            'Rolling_Max_Close', 
                            'Rolling_Min_Close',
                            "Rolling_Mean_Close"]:
            df[f"{start_price}__to__{change_price}__Relative_Difference"] = (df[change_price] - df[start_price]) / df[start_price]
    return df


def _get_number_of_positive_events(df_candles, hp, threshold, column_to_track):
    """
    Returns amount of events where the price exceeded some threshold

    Parameters
    ----------
    df_candles : pd.DataFrame
        OHLC dataframe with columns [Timestamp, Open, High, Low, Close, Volume]
    hp : datetime.timedelta
        hold period
    threshold : float
        threshold to exceed
    column_to_track : str
        A column to use for price traching.
        Possible values:
            'Close__to__Rolling_Max_High__Relative_Difference',
            'Close__to__Rolling_Min_Low__Relative_Difference',
            'Close__to__Rolling_Max_Close__Relative_Difference',
            'Close__to__Rolling_Min_Close__Relative_Difference',
            'Close__to__Rolling_Mean_Close__Relative_Difference',
            'Low__to__Rolling_Max_High__Relative_Difference',
            'Low__to__Rolling_Min_Low__Relative_Difference',
            'Low__to__Rolling_Max_Close__Relative_Difference',
            'Low__to__Rolling_Min_Close__Relative_Difference',
            'Low__to__Rolling_Mean_Close__Relative_Difference',
            'High__to__Rolling_Max_High__Relative_Difference',
            'High__to__Rolling_Min_Low__Relative_Difference',
            'High__to__Rolling_Max_Close__Relative_Difference',
            'High__to__Rolling_Min_Close__Relative_Difference',
            'High__to__Rolling_Mean_Close__Relative_Difference'

    Returns
    -------
    tuple
        number_of_events, population_size
    """
    df = _get_data_for_analysis(df_candles=df_candles, hp=hp)
    number_of_events = df[df[column_to_track] >= threshold].shape[0]
    population_size = df.shape[0]
    return number_of_events, population_size


def _get_number_of_negative_events(df_candles, hp, threshold, column_to_track):
    """
    Returns amount of events where the price dips below some threshold

    Parameters
    ----------
    df_candles : pd.DataFrame
        OHLC dataframe with columns [Timestamp, Open, High, Low, Close, Volume]
    hp : datetime.timedelta
        hold period
    threshold : float
        threshold to dip below
        naturally to be negative
    column_to_track : str
        A column to use for price traching.
        Possible values:
            'Close__to__Rolling_Max_High__Relative_Difference',
            'Close__to__Rolling_Min_Low__Relative_Difference',
            'Close__to__Rolling_Max_Close__Relative_Difference',
            'Close__to__Rolling_Min_Close__Relative_Difference',
            'Close__to__Rolling_Mean_Close__Relative_Difference',
            'Low__to__Rolling_Max_High__Relative_Difference',
            'Low__to__Rolling_Min_Low__Relative_Difference',
            'Low__to__Rolling_Max_Close__Relative_Difference',
            'Low__to__Rolling_Min_Close__Relative_Difference',
            'Low__to__Rolling_Mean_Close__Relative_Difference',
            'High__to__Rolling_Max_High__Relative_Difference',
            'High__to__Rolling_Min_Low__Relative_Difference',
            'High__to__Rolling_Max_Close__Relative_Difference',
            'High__to__Rolling_Min_Close__Relative_Difference',
            'High__to__Rolling_Mean_Close__Relative_Difference'

    Returns
    -------
    tuple
        number_of_events, population_size
    """
    df = _get_data_for_analysis(df_candles=df_candles, hp=hp)
    number_of_events = df[df[column_to_track] < threshold].shape[0]
    population_size = df.shape[0]
    return number_of_events, population_size
