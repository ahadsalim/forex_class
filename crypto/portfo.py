import json
import threading
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

def thread1() : 
    conn = sqlite3.connect(db_name)
    coinex = Coinex.Coinex_API(api_key, api_secret, connection=conn , client_id=client_id)
    coinex.buy_portfo(num_symbols=3, cash=25,percent_of_each_symbol=0.2, interval="5min",higher_interval="15min",HMP_candles=num_candles)
    conn.close()

def thread2():
    conn2 = sqlite3.connect(db_name)
    coinex2 = Coinex.Coinex_API(api_key, api_secret, connection=conn2 , client_id=client_id)
    while True :
        if coinex2.check_portfo(loss_limit=loss_limit,client_id=client_id):
            print("Every thing is OK.")
        else :
            print("Pay attention to the errors !!!")
    conn2.close()

if __name__ == '__main__':
    t1 = threading.Thread(target=thread1)
    t2 = threading.Thread(target=thread2)

    t1.start()
    t2.start()

    t1.join()
    t2.join()