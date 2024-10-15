import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm


from bte.utils.binance_candle_data import extract_data as extract_price_candles_data


def plot_hist_with_normal(data, bins):
    """
    Function to plot a histogram with a normal distribution line.
    
    Parameters:
    data (list or array): List of numerical values.
    bins (int): Number of bins for the histogram.
    """
    # Calculate mean and standard deviation
    df = pd.DataFrame()
    df["c"] = data
    mean = df["c"].mean()
    std = df["c"].std()

    # Create the histogram
    plt.hist(data, bins=bins, density=True, alpha=0.6, color='b', edgecolor='black')

    # Generate the x values for the normal distribution line plot
    xmin, xmax = plt.xlim()  # Get the range of x from the histogram
    x = np.linspace(xmin, xmax, 100)

    # Plot the normal distribution using the calculated mean and std
    p = norm.pdf(x, mean, std)
    plt.plot(x, p, 'k', linewidth=2)

    # Add title with mean and standard deviation
    plt.title(f'Histogram with Normal Distribution\nMean = {mean:.4f}, Std = {std:.4f}')

    # Add labels and grid
    plt.xlabel('Value')
    plt.ylabel('Density')
    plt.grid(True)

    # Display the plot
    plt.show()


def get_simulation_data(df_price, 
                        hp, 
                        iterations=100000, 
                        iteration_size=15):
    low_simulation_list = []
    high_simulation_list = []
    for i in range(iterations):
        print(f"{datetime.datetime.now()}: {i}/{iterations} = {i/iterations*100}%")
        entries = df_price.sample(n=iteration_size)
        entry_points = entries["Timestamp"].values
        entry_prices = entries["Close"].values
        price_diff_low = []
        price_diff_high = []
        for entry_time, entry_price in zip(entry_points, entry_prices):
            df = df_price[( df_price["Timestamp"] > entry_time) & (df_price["Timestamp"] - pd.Timedelta(hours=hp) <= entry_time)]
            min_price = df["Low"].min()
            max_price = df["High"].max()
            price_diff_low.append((min_price - entry_price ) / entry_price)
            price_diff_high.append((max_price - entry_price) / entry_price)
        low_simulation_list.extend(price_diff_low)
        high_simulation_list.extend(price_diff_high)
    return low_simulation_list, high_simulation_list


pair = "BTCUSDT"
from_date="2024-05-20 00:00:00"
to_date="2024-09-19 00:00:00"
iterations = 100000
iteration_size = 15

df_price = extract_price_candles_data(symbol=pair,
                                      from_date=from_date, 
                                      to_date=to_date, 
                                      c_size="1h")

hp = 24
low_simulation_list, high_simulation_list = get_simulation_data(df_price, 
                                                                hp, 
                                                                iterations, 
                                                                iteration_size)
print(f"""
####################################################
      Low Distribution; hp: {hp}h
####################################################""")
plot_hist_with_normal(data=low_simulation_list, bins=100)

print(f"""
####################################################
      High Distribution; hp: {hp}h
####################################################""")
plot_hist_with_normal(data=high_simulation_list, bins=100)

print(f"""
####################################################
      Joined Distribution; hp: {hp}h
####################################################""")
plot_hist_with_normal(data=low_simulation_list + high_simulation_list, bins=100)

hp = 1
low_simulation_list, high_simulation_list = get_simulation_data(df_price, 
                                                                hp, 
                                                                iterations, 
                                                                iteration_size)
print(f"""
####################################################
      Low Distribution; hp: {hp}h
####################################################""")
plot_hist_with_normal(data=low_simulation_list, bins=100)

print(f"""
####################################################
      High Distribution; hp: {hp}h
####################################################""")
plot_hist_with_normal(data=high_simulation_list, bins=100)

print(f"""
####################################################
      Joined Distribution; hp: {hp}h
####################################################""")
plot_hist_with_normal(data=low_simulation_list + high_simulation_list, bins=100)




