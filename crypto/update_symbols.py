# This file update potential symbols for trading from Coinex Exchange
import json
import sqlite3
import Coinex_API_Class as Coinex
import schedule
import time

try :
    with open('crypto/config.json', 'r') as f:
        data = json.load(f)
        api_key = data['api_key']
        api_secret = data['api_secret']
        loss_limit = data['loss_limit']
        num_candles = data['num_candle']
        db_name = data['db_name']
        min_price_symbol = data['min_price_symbol']
except Exception as e:
    print(f"Error: {e}")

with sqlite3.connect(db_name) as conn:
    coinex = Coinex.Coinex_API(api_key, api_secret, conn)
    schedule.every().day.at("00:00").do(coinex.filter_spot_market(min_price_symbol))

while True:
  schedule.run_pending()
  time.sleep(60)