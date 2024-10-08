import json
import sqlite3
import Coinex_API_Class as Coinex

try :
    with open('crypto/config.json', 'r') as f:
        data = json.load(f)
        api_key = data['api_key']
        api_secret = data['api_secret']
        num_candles = data['num_candle']
        db_name = data['db_name']
except Exception as e:
    print(f"Error: {e}")

conn = sqlite3.connect(db_name)
coinex = Coinex.Coinex_API(api_key, api_secret, conn)

coinex.buy_portfo(3,25,0.2,"5min","15min",num_candles)

conn.close()