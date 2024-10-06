import pandas as pd
import Coinex_API_Class as Coinex

api_key = "47D7C4B286224298BB3D88A9D7161A45"
api_secret = "F0F2D4156F6892ED2F03576FEE74CFA658C5F8271DCE102D"
coinex = Coinex.Coinex_API(api_key,api_secret)
loss_limit = 0.9

portfo= pd.read_csv("portfo.csv")
for index, row in portfo.iterrows():
    price = portfo.loc[index,"price"].astype(float)
    last_price=float(coinex.get_spot_price_ticker(row["market"])[0]["last"])
    if (last_price < price * loss_limit) :
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