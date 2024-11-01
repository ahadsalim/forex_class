import json
import time
import sqlite3
import Coinex_API_Class as Coinex

try :
    with open('crypto/config.json', 'r') as f:
        data = json.load(f)
        api_key = data['api_key']
        api_secret = data['api_secret']
        db_name = data['db_name']
        num_candles = data['num_candle']
        loss_limit = data['loss_limit']
        client_id = data['client_id']
        interval = data['interval']
        sleep_check = data['sleep_check']
        higher_interval = data['higher_interval']
except Exception as e:
    print(f"Error: {e}")

conn = sqlite3.connect(db_name)
coinex = Coinex.Coinex_API(api_key, api_secret, conn)
while True :
    coinex.make_portfo(num_symbols=5, cash=35,percent_of_each_symbol=0.2, interval=interval, higher_interval=higher_interval, HMP_candles=num_candles ,client_id=client_id)
    time.sleep(sleep_check)
conn.close()