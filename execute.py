from tradingview_ta import TA_Handler, Interval, Exchange , TradingView
import Coinex_API_Class as CoinexAPI
import pandas as pd
from tqdm import tqdm

api_key = "47D7C4B286224298BB3D88A9D7161A45"
api_secret = "F0F2D4156F6892ED2F03576FEE74CFA658C5F8271DCE102D"
coinex = CoinexAPI.Coinex_API(api_key,api_secret)

def prepare_data (period,limit) :
    data_spot = coinex.get_spot_cum_ret(period,limit)
    data_spot['Recomandation'] = None
    data_spot['Buy'] = None
    data_spot['Sell'] = None
    data_spot['Neutral'] = None
    if period== "1min" : preiod_ta = "INTERVAL_1_MINUTE"
    elif period == "5min" : preiod_ta = "INTERVAL_5_MINUTES"
    elif period == "15min" : preiod_ta = "INTERVAL_15_MINUTES"
    elif period == "30min" : preiod_ta = "INTERVAL_30_MINUTES"
    elif period == "1hour" : preiod_ta = "INTERVAL_1_HOUR"
    elif period == "2hour" : preiod_ta = "INTERVAL_2_HOUR"
    elif period == "4hour" : preiod_ta = "INTERVAL_4_HOUR"
    elif period == "1day" : preiod_ta = "INTERVAL_1_DAY"
    elif period == "1week" : preiod_ta = "INTERVAL_1_WEEK"
    
    for index, row in tqdm(data_spot.iterrows(), total=len(data_spot), desc="Processing symbol" , position=0):
        try : 
            if period== "1min" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_MINUTE)).get_analysis().summary
            elif period == "5min" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_5_MINUTES)).get_analysis().summary
            elif period == "15min" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_15_MINUTES)).get_analysis().summary
            elif period == "30min" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_30_MINUTES)).get_analysis().summary
            elif period == "1hour" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_HOUR)).get_analysis().summary
            elif period == "2hour" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_2_HOURS)).get_analysis().summary
            elif period == "4hour" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_4_HOURS)).get_analysis().summary
            elif period == "1day" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_DAY)).get_analysis().summary
            elif period == "1week" : 
                data= (TA_Handler(symbol=row['symbol'], screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_WEEK)).get_analysis().summary
            data_spot.loc[index, 'Recomandation'] =data['RECOMMENDATION']
            data_spot.loc[index, 'Buy'] = data['BUY']
            data_spot.loc[index, 'Sell'] = data['SELL']
            data_spot.loc[index, 'Neutral'] = data['NEUTRAL']
            print(index)
        except Exception as e:
            continue
        df=data_spot.drop(columns=["Open","High","Low","Return"] , axis=1)
    return df

df= prepare_data("5min",10)
df.to_csv('data.csv', index=False)
