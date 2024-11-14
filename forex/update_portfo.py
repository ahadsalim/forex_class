import json
import time
import MT5_API_Class as MT5_Class

try :
    with open('forex/config.json', 'r') as f:
        data = json.load(f)
        username = data['username']
        password = data['password']
        exchange_server = data['exchange_server']
        sleep_check = data['sleep_check']
        num_candles = data['num_candle']
        stop_loss = data['stop_loss']
        take_profit = data['take_profit']
        interval = data['interval']
        higher_interval = data['higher_interval']
except Exception as e:
    print(f"Error: {e}")

forx= MT5_Class.MT5_API(username,password,exchange_server)
forx.initialize()
while True:
    forx.check_portfo(5,interval, higher_interval, num_candles, lot=0.01, stop_loss=stop_loss, take_profit=take_profit, deviation =5 )
    time.sleep(sleep_check)
forx.shutdown()

