import hashlib
import json
import time
import hmac
import requests
from tqdm import tqdm
import pandas as pd
import numpy as np
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
        return "This class use to connect CoinEx Exchange"
    
    def __init__(self , access_id , secret_key):
        self.access_id = access_id
        self.secret_key = secret_key
        self.url = "https://api.coinex.com/v2"
        self.headers = self.HEADERS.copy()

    # Generate your signature string
    def gen_sign(self, method, request_path, body, timestamp):
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
            Coin specifications in the spot market
            Arguman :
                null : Get all Coins exchange 
                    Return in Dataframe format
                ticker : To get detail of coin exchange like : BTCUSDT  
                    Retun in json format
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
            Get Price of ticker in spot market
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
                df2["Ticker"] = ticker
                df2['Time'] = pd.to_datetime(df['created_at'], unit='ms')
                df2['Open'] = df["open"].astype(float)
                df2['High'] = df["high"].astype(float)
                df2['Low'] = df["low"].astype(float)
                df2['Close'] = df["close"].astype(float)
                df2['PrcntChange'] = np.log(df["close"].astype(float) / df["close"].astype(float).shift(1))
                df2['Volume'] = df["volume"].astype(float)
                df2['Value'] = df["value"].astype(float)
                return df2
            else :
                df2["Ticker"] = ticker
                df2['Time'] = 0
                df2['Open'] = 0
                df2['High'] = 0
                df2['Low'] = 0
                df2['Close'] = 0
                df2['PrcntChange'] = 0
                df2['Volume'] = 0
                df2['Value'] = 0
                return df2
        else :
            raise ValueError(res["message"])

    def get_spot_pctchange(self,period,limit) :
        markets = self.get_spot_market()
        markets = [symbol for symbol in markets["market"].to_list() if symbol.endswith('USDT')]
        df = pd.DataFrame()
        for symbol in tqdm(markets) :
            data=self.get_spot_kline(symbol,period,limit)
            print (data)
            df = pd.concat([df, data], ignore_index=True)
            print (df)

        return df
            
        

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
