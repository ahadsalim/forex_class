import pandas as pd
import json
import Coinex_API_Class as Coinex

try :
    with open('config.json', 'r') as f:
        data = json.load(f)
        api_key = data['api_key']
        api_secret = data['api_secret']
        loss_limit = data['loss_limit']
        num_candles = data['num_candle']
except Exception as e:
    print(f"Error: {e}")

coinex = Coinex.Coinex_API(api_key,api_secret)

portfo= pd.read_csv("portfo.csv")
for index, row in portfo.iterrows():
    price = portfo.loc[index,"price"].astype(float)
    last_price=float(coinex.get_spot_price_ticker(row["market"])[0]["last"])
    if (last_price < price * loss_limit) :
        # if price in under loss limit, sell it
        stat ,res= coinex.put_spot_order(row['market'], "sell", "market", row["amount"])
        if (stat == "done") :
            df = pd.json_normalize(res["data"])
            print(df)
            portfo = portfo.drop(index, axis=0) # delete symbol from portfo
            portfo.to_csv("portfo.csv", index=False)
        else :
            print("Error in placing order",stat,row['market'],res)
    elif (last_price > price * 1.1) :
        print("# update price")