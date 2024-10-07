import pandas as pd
import json
import Coinex_API_Class as Coinex

try :
    with open('config.json', 'r') as f:
        data = json.load(f)
        api_key = data['api_key']
        api_secret = data['api_secret']
        coinex = Coinex.Coinex_API(api_key,api_secret)
        loss_limit = data['loss_limit']
except Exception as e:
    print(f"Error: {e}")

def symbol_Candidates(num_symbols, interval, higher_interval , num_candles):
    """
    Select top num_symbols symbols that have a strong buy signal for both the given interval and the higher interval.

    Parameters
    ----------
    num_symbols : int
        The number of symbols to select.
    interval : str
        The interval on which to select symbols. Can be '1min', '5min', '15min', '30min', '1hour', '2hour', '4hour', '1day', '1week'.
    higher_interval : str
        The higher interval on which to select symbols. Should be higher than the given interval.
    num_candles : int
        The number of candles to use for the selection.

    Returns
    -------
    str, pd.DataFrame
        The first element is a string indicating the status of the selection. Can be "empty", "uncomplete", or "Full". The second element is a dataframe containing the selected symbols with their respective data.
    """
    tickers_df = coinex.get_ta_filtered_tickers(interval, num_candles)
    tickers_df2 = tickers_df[(tickers_df['Recomandation'] == "STRONG_BUY")]
    higher_tickers_df = coinex.get_ta_filtered_tickers(higher_interval, num_candles)
    higher_tickers_df2 = higher_tickers_df[(higher_tickers_df['Recomandation'] == "STRONG_BUY")]
    shared_tickers_df = pd.merge(tickers_df2, higher_tickers_df2, on='symbol', how='inner')
    top_symbols = shared_tickers_df.sort_values('Cum_Return_x', ascending=False).head(num_symbols)
    if len(top_symbols) == 0 :
        return "empty" , top_symbols
    elif (len(top_symbols) < num_symbols):
        top_symbols.drop(columns=['maker_fee_rate_x', 'maker_fee_rate_y', 'taker_fee_rate_y', 'Return_x', 'Return_y',
                                   'Value_x', 'Value_y','Volume_y','period_x','limit_x','Cum_Return_y', 'Cum_Value_y', 
                                   'Time_y','min_amount_y','Close_y','period_y', 'limit_y', 'Recomandation_y', 'Buy_y', 'Sell_y', 'Neutral_y'], inplace=True)
        return "uncomplete" , top_symbols
    else :
        top_symbols.drop(columns=['maker_fee_rate_x', 'maker_fee_rate_y', 'taker_fee_rate_y', 'Return_x', 'Return_y',
                                   'Value_x', 'Value_y','Volume_y','period_x','limit_x','Cum_Return_y', 'Cum_Value_y',
                                   'Time_y','min_amount_y','Close_y','period_y', 'limit_y', 'Recomandation_y', 'Buy_y', 'Sell_y', 'Neutral_y'], inplace=True)
        return "full" , top_symbols

def buy_portfo(num_symbols ,stock , percent_of_each_symbol , interval, higher_interval ) :
    """
    Create a portfolio of symbols with a strong buy signal for both the given interval and the higher interval.

    Parameters
    ----------
    stock : float
        The total stock to divide among the selected symbols.
    percent_of_each_symbol : float
        The percentage of the total stock to allocate to each symbol.
    interval : str
        The interval on which to select symbols. Can be '1min', '5min', '15min', '30min', '1hour', '2hour', '4hour', '1day', '1week'.
    higher_interval : str
        The higher interval on which to select symbols. Should be higher than the given interval.

    Returns
    -------
    pd.DataFrame
        The portfolio as a dataframe. If no symbols are found, return "No symbols found".
    """
    portfo = pd.read_csv("portfo.csv")
    while portfo.shape[0] <=5 :
        status , df = symbol_Candidates(num_symbols= num_symbols , interval= interval, higher_interval=higher_interval, num_candles=50)
        if status != "empty" :
            
            for index, row in df.iterrows():
                amount = max(stock * percent_of_each_symbol / row['Close_x'], row['min_amount_x'])
                stat ,res= coinex.put_spot_order(row['symbol'], "buy", "market", amount)
                if (stat == "done") :
                    df = pd.json_normalize(res["data"])
                    if df.empty:
                        portfo = df.copy()
                    else:
                        portfo = pd.concat([portfo, df], ignore_index=True)
                else :
                    print("Error in placing order",stat,row['symbol'],res)
        else :
            print("No symbols found")
        portfo.to_csv("portfo.csv")
    return portfo

print (buy_portfo(5,10,0.1,"1min","5min"))