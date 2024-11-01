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
        take_profit = data['take_profit']
        client_id = data['client_id']
        interval = data['interval']
        sleep_check = data['sleep_check']
        higher_interval = data['higher_interval']
except Exception as e:
    print(f"Error: {e}")

conn = sqlite3.connect(db_name)

coinex = Coinex.Coinex_API(api_key, api_secret, connection=conn , client_id=client_id)
while True :
    coinex.check_portfo(loss_limit=loss_limit,take_profit=take_profit ,client_id=client_id, interval=interval , take_profit=take_profit)
    time.sleep(sleep_check)
conn.close()