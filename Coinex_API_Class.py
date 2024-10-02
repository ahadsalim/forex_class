import hashlib
import json
import time
import hmac
import requests
from tqdm import tqdm
import pandas as pd
import numpy as np
from tradingview_ta import TA_Handler, Interval, Exchange , TradingView
from urllib.parse import urlparse

class Coinex_API(object):
    
    HEADERS = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "X-COINEX-KEY": "",
        "X-COINEX-SIGN": "",
        "X-COINEX-TIMESTAMP": "",
    }
    def __repr__(self): 
        """
        Connects to the CoinEx API version 2

        Args:
            api_key (str): Your CoinEx API key.
            api_secret (str): Your CoinEx API secret.
        """
        return "This class use to work with CoinEx Exchange site"
    
    def __init__(self , access_id , secret_key):
        '''
            Set initial values.
        '''
        self.access_id = access_id
        self.secret_key = secret_key
        self.url = "https://api.coinex.com/v2"
        self.headers = self.HEADERS.copy()

    def gen_sign(self, method, request_path, body, timestamp):
        '''
            Generate your signature string
        '''
        prepared_str = f"{method}{request_path}{body}{timestamp}"
        signature = hmac.new(
            bytes(self.secret_key, 'latin-1'), 
            msg=bytes(prepared_str, 'latin-1'), 
            digestmod=hashlib.sha256
        ).hexdigest().lower()
        return signature

    def get_common_headers(self, signed_str, timestamp):
        headers = self.HEADERS.copy()
        headers["X-COINEX-KEY"] = self.access_id
        headers["X-COINEX-SIGN"] = signed_str
        headers["X-COINEX-TIMESTAMP"] = timestamp
        headers["Content-Type"] = "application/json; charset=utf-8"
        return headers

    def request(self, method, url, params={}, data=""):
        '''
            Create request URL to get & set data
            Return : Answer from CoinEx server
        '''
        req = urlparse(url)
        request_path = req.path

        timestamp = str(int(time.time() * 1000))
        if method.upper() == "GET":
            # If params exist, query string needs to be added to the request path
            if params:
                query_params = []
                for item in params:
                    if params[item] is None:
                        continue
                    query_params.append(item + "=" + str(params[item]))
                query_string = "?{0}".format("&".join(query_params))
                request_path = request_path + query_string

            signed_str = self.gen_sign(
                method, request_path, body="", timestamp=timestamp
            )
            response = requests.get(
                url,
                params=params,
                headers=self.get_common_headers(signed_str, timestamp),
            )

        else:
            signed_str = self.gen_sign(
                method, request_path, body=data, timestamp=timestamp
            )
            response = requests.post(
                url, data, headers=self.get_common_headers(signed_str, timestamp)
            )

        if response.status_code != 200:
            raise ValueError(response.text)
        return response
    
    def get_spot_market(self,ticker=""):
        '''
            Get information about ticker in spot market in CoinEx site
            Arguman :
                null : Get all tickers in exchange Return in Dataframe format
                ticker : To get detail of ticker in exchange like : BTCUSDT  Retun in json format
        '''
        request_path = "/spot/market"
        params = {"market": ticker}
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res=response.json()
        if res["code"]==0 :
            if ticker=="" :
                df = pd.json_normalize(res["data"])
                return df
            else:
                return res["data"]
        else :
            raise ValueError(res["message"])

    def get_spot_price_ticker(self,ticker):
        '''
            Get price info of the ticker among 24 hours ago in spot market
        '''
        request_path = "/spot/ticker"
        params = {"market": ticker}
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res=response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])

    def save_filtered_spot_market (self):
        '''
            This function updates the symbols information on the CoinEx site every 24 hours and saves it in the tickers.csv file.
        '''
        df = self.get_spot_market()
        data = df[df['market'].str.endswith('USDT')].copy()
        data.drop(columns=["base_ccy","base_ccy_precision","quote_ccy","quote_ccy_precision"] , inplace=True)
        data = data.reindex(columns=['market','min_amount','maker_fee_rate','taker_fee_rate','is_amm_available','is_margin_available'])
        for index,symbol in tqdm(data.iterrows()) :
            try :
                info = self.get_spot_price_ticker(symbol['market'])[0]
                data.loc[index,"price"]= info['last']
                data.loc[index,"value"]= info['value']
                data.loc[index,"volume_sell"]= info['volume_sell']
                data.loc[index,"volume_buy"]= info['volume_buy']
            except Exception as e:
                print (e)
                data.loc[index,"price"]= 0
                data.loc[index,"value"]= 0
                data.loc[index,"volume_sell"]= 0
                data.loc[index,"volume_buy"]= 0
        try :
            data.to_csv("tickers.csv" , index=False)
        except Exception as e:
            print("Can't save data !!!" , e)
        
        return data

    def get_spot_kline(self,ticker,period,limit):
        '''
            ticker : ticker name
            limit : Number of transaction data items. Default as 100, max. value 1000
            period :One of ["1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", "6hour", "12hour", "1day", "3day", "1week"]

            return : DataFrame
        '''
        request_path = "/spot/kline"
        params = {"market": ticker , "period" : period , "limit": limit }
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res=response.json()
        df2= pd.DataFrame()
        if res["code"]==0 :
            df = pd.json_normalize(res["data"])
            if df.shape[0] != 0 :  # If there is any data for this ticker
                df2['Time'] = pd.to_datetime(df['created_at'], unit='ms')
                df2['Open'] = df["open"].astype(float)
                df2['High'] = df["high"].astype(float)
                df2['Low'] = df["low"].astype(float)
                df2['Close'] = df["close"].astype(float)
                df2['Return'] = np.log(df["close"].astype(float) / df["close"].astype(float).shift(1))
                df2["Cum_Return"] = np.exp(df2["Return"].cumsum())
                df2['Volume'] = df["volume"].astype(float)
                df2['Value'] = df["value"].astype(float)
                df2['Cum_Value']= df2['Value'].cumsum()
                return df2
            else :
                return False
        else :
            raise ValueError(res["message"])

    def get_cum_ret_filtered_tickers(self,period,limit) :
        '''
            Get Cumulative Return in the period in limit time for all tickers in tickers.csv
            tradingview : check tradingview site 
        '''
        markets = pd.read_csv("tickers.csv")
        df=pd.DataFrame(columns=['Time',"symbol","min_amount","maker_fee_rate","taker_fee_rate",
                                 'Close','Return','Cum_Return','Volume','Value','Cum_Value','period','limit'])
        for index,symbol in tqdm(markets.iterrows()) :
            ticker= symbol['market']
            try :
                data=self.get_spot_kline(ticker,period,limit).iloc[-1]
                info ={"Time" : [data["Time"]] , "symbol": [ticker] , "min_amount": [symbol["min_amount"]] , 
                       "maker_fee_rate": [symbol["maker_fee_rate"]] ,"taker_fee_rate": [symbol["taker_fee_rate"]],
                       "Close": [data['Close']], "Return": [data['Return']], "Cum_Return":[data["Cum_Return"]] ,
                       "Volume": [data['Volume']] ,"Value": [data["Value"]] ,"Cum_Value": [data['Cum_Value']] , "period":[period] , "limit":[limit] }
                info = pd.DataFrame(info)
                if df.empty :
                    df =info.copy()
                else :
                    df = pd.concat([df, info],ignore_index=True)
            except Exception as e :
                print(e)
                continue
        return df
    
    def get_ta_filtered_tickers (self,period,limit) :
        data_spot = self.get_cum_ret_filtered_tickers(period,limit)
        data_spot['Recomandation'] = None
        data_spot['Buy'] = None
        data_spot['Sell'] = None
        data_spot['Neutral'] = None
        for index, row in tqdm(data_spot.iterrows(), total=len(data_spot), desc="Processing symbol" , position=0):
            symbol=row['symbol']
            try : 
                if period== "1min" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_MINUTE)).get_analysis().summary
                elif period == "5min" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_5_MINUTES)).get_analysis().summary
                elif period == "15min" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_15_MINUTES)).get_analysis().summary
                elif period == "30min" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_30_MINUTES)).get_analysis().summary
                elif period == "1hour" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_HOUR)).get_analysis().summary
                elif period == "2hour" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_2_HOURS)).get_analysis().summary
                elif period == "4hour" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_4_HOURS)).get_analysis().summary
                elif period == "1day" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_DAY)).get_analysis().summary
                elif period == "1week" : 
                    data= (TA_Handler(symbol=symbol, screener="crypto", exchange="COINEX", interval=Interval.INTERVAL_1_WEEK)).get_analysis().summary
                data_spot.loc[index, 'Recomandation'] =data['RECOMMENDATION']
                data_spot.loc[index, 'Buy'] = data['BUY']
                data_spot.loc[index, 'Sell'] = data['SELL']
                data_spot.loc[index, 'Neutral'] = data['NEUTRAL']
            except Exception as e:
                print("Error:",e)
                continue
        return data_spot
    
    def get_spot_balance(self):
        '''
            Get balance of your account in jason format
        '''
        request_path = "/assets/spot/balance"
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
        )
        res=response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])
        
    def get_deposit_address(self,currency,chain):
        '''
        Get your Wallet Address for deposit
        currency as string
        chain as string ("TRC20" , "CSC" , "BEP20" , ...)
        '''
        request_path = "/assets/deposit-address"
        params = {"ccy": currency, "chain": chain}

        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res=response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])
        
    def put_limit(self):
        request_path = "/spot/order"
        data = {
            "market": "BTCUSDT",
            "market_type": "SPOT",
            "side": "buy",
            "type": "limit",
            "amount": "10000",
            "price": "1",
            "client_id": "user1",
            "is_hide": True,
        }
        data = json.dumps(data)
        response = self.request(
            "POST",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            data=data,
        )
        return response.json()
