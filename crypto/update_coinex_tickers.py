import json
import Coinex_API_Class as Coinex
    
try :
    with open('config.json', 'r') as f:
        data = json.load(f)
        api_key = data['api_key']
        api_secret = data['api_secret']
        coinex = Coinex.Coinex_API(api_key,api_secret)
        loss_limit = data['loss_limit']
except Exception as e:
    print(f"Error: {e}")
    
ret = coinex.save_filtered_spot_market(0.2)
if  ret == True:
    # Notify me this is updated !
    print("OK")
else :
    # Notify me the error !!!!
    print(ret)