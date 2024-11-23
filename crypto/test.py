import json
import sqlite3
import pandas as pd
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

with sqlite3.connect(db_name) as conn:
    coinex = Coinex.Coinex_API(api_key, api_secret, conn)
    coinex.calculate_profit()