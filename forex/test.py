#from datetime import datetime
#import matplotlib.pyplot as plt
#import pandas as pd
import sqlite3
import json
#from pandas.plotting import register_matplotlib_converters
#register_matplotlib_converters()
from tradingview_ta import TradingView
import MetaTrader5 as mt5
import MT5_API_Class as MT5_Class

try :
    with open('forex/config.json', 'r') as f:
        data = json.load(f)
        username = data['username']
        password = data['password']
        exchange_server = data['exchange_server']
        sleep_check = data['sleep_check']
        db_name = data['db_name']
        num_candles = data['num_candle']
        loss_limit = data['loss_limit']
        interval = data['interval']
        higher_interval = data['higher_interval']
except Exception as e:
    print(f"Error: {e}")

conn = sqlite3.connect(db_name)
forx= MT5_Class.MT5_API(username,password,exchange_server,conn)
forx.initialize()
forx.make_portfo(5,interval, higher_interval, num_candles, lot=0.01, stop_loss=25, take_profit=50, deviation =20 )
while True:
    forx.check_portfo(stop_loss=25, take_profit=50 )
forx.shutdown()


'''
# request 1000 ticks from EURAUD
euraud_ticks = mt5.copy_ticks_from("EURAUD", datetime(2024,1,28,13), 1000, mt5.COPY_TICKS_ALL)
# request ticks from AUDUSD within 2019.04.01 13:00 - 2019.04.02 13:00
audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(2024,1,27,13), datetime(2024,1,28,13), mt5.COPY_TICKS_ALL)
 
# get bars from different symbols in a number of ways
eurusd_rates = mt5.copy_rates_from("EURUSD", mt5.TIMEFRAME_M1, datetime(2024,1,28,13), 1000)
eurgbp_rates = mt5.copy_rates_from_pos("EURGBP", mt5.TIMEFRAME_M1, 0, 1000)
eurcad_rates = mt5.copy_rates_range("EURCAD", mt5.TIMEFRAME_M1, datetime(2024,1,27,13), datetime(2024,1,28,13))
 
# shut down connection to MetaTrader 5
mt5.shutdown()
 
#DATA
print('euraud_ticks(', len(euraud_ticks), ')')
for val in euraud_ticks[:10]: print(val)
 
print('audusd_ticks(', len(audusd_ticks), ')')
for val in audusd_ticks[:10]: print(val)
 
print('eurusd_rates(', len(eurusd_rates), ')')
for val in eurusd_rates[:10]: print(val)
 
print('eurgbp_rates(', len(eurgbp_rates), ')')
for val in eurgbp_rates[:10]: print(val)
 
print('eurcad_rates(', len(eurcad_rates), ')')
for val in eurcad_rates[:10]: print(val)
 
#PLOT
# create DataFrame out of the obtained data
ticks_frame = pd.DataFrame(euraud_ticks)
# convert time in seconds into the datetime format
ticks_frame['time']=pd.to_datetime(ticks_frame['time'], unit='s')
# display ticks on the chart
plt.plot(ticks_frame['time'], ticks_frame['ask'], 'r-', label='ask')
plt.plot(ticks_frame['time'], ticks_frame['bid'], 'b-', label='bid')
 
# display the legends
plt.legend(loc='upper left')
 
# add the header
plt.title('EURAUD ticks')
 
# display the chart
plt.show()
'''