import json
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
except Exception as e:
    print(f"Error: {e}")

conn = sqlite3.connect(db_name)
coinex = Coinex.Coinex_API(api_key, api_secret, connection=conn , client_id=client_id)
while True :
    if coinex.check_portfo(loss_limit=loss_limit,client_id=client_id):
        print("Every thing is OK.")
    else :
        print("Pay attention to the errors !!!")

conn.close()