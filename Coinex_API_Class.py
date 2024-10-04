import hashlib
import json
import time
import hmac
import requests
from tqdm import tqdm
import pandas as pd
import numpy as np
from typing import Union
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
    
    def get_spot_market(self, ticker: str = "") -> Union[pd.DataFrame, dict]:
        '''
            Get information about ticker in spot market in CoinEx site
            Args:
                ticker (str): Optional, to get detail of ticker in exchange like : BTCUSDT. Default is "".
            Returns:
                Union[pd.DataFrame, dict]: If ticker is "" then it returns all tickers in exchange in DataFrame format.
                                            If ticker is not "" then it returns detail of ticker in exchange in dict format.
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
    def save_filtered_spot_market (self,min_price):
        '''
            This function updates the symbols information that price is upper than min_price every 24 hours and saves it in the tickers.csv file.
            It only selects the symbols that end with 'USDT' and uses the last price to filter the symbols.
            It also drops the columns that are not necessary for the analysis.
            It updates the price, value, volume_sell, volume_buy columns for each symbol.
            It then filters the symbols with price above min_price.
            Finally, it saves the DataFrame to the tickers.csv file.
        '''
        df = self.get_spot_market()
        # Select only the symbols that end with 'USDT'
        data = df[df['market'].str.endswith('USDT')].copy()
        # Drop the columns that are not necessary for the analysis
        data.drop(columns=["base_ccy","base_ccy_precision","quote_ccy","quote_ccy_precision"] , inplace=True)
        # Reindex the columns to match the order of the columns in the DataFrame
        data = data.reindex(columns=['market','min_amount','maker_fee_rate','taker_fee_rate','is_amm_available','is_margin_available'])
        # Iterate over each symbol and update the price, value, volume_sell, volume_buy columns
        for index,symbol in tqdm(data.iterrows() ,total=len(data)) :
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
        # Filter the symbols with price above min_price
        df= data[(data['price'].astype(float)> min_price)]
        try :
            # Save the DataFrame to the tickers.csv file
            df.to_csv("tickers.csv" , index=False)
            return True
        except Exception as e:
            print("Can't save data !!!" , e)
            return e

    def get_spot_kline(self, ticker, period, limit):
        """
        Get the last {limit} kline data of {ticker} in {period} period

        Args:
            ticker (str): Ticker name
            period (str): One of ["1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", "6hour", "12hour", "1day", "3day", "1week"]
            limit (int): Number of transaction data items. Default as 100, max. value 1000

        Returns:
            pd.DataFrame: DataFrame with the following columns:
                Time (datetime64[ns]): Time of the kline data
                Open (float): Open price of the kline data
                High (float): High price of the kline data
                Low (float): Low price of the kline data
                Close (float): Close price of the kline data
                Return (float): Return of the kline data
                Cum_Return (float): Cumulative return of the kline data
                Volume (float): Volume of the kline data
                Value (float): Value of the kline data
                Cum_Value (float): Cumulative value of the kline data
        """
        request_path = "/spot/kline"
        params = {"market": ticker, "period": period, "limit": limit}
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res = response.json()
        df2 = pd.DataFrame()
        if res["code"] == 0:
            df = pd.json_normalize(res["data"])
            if df.shape[0] != 0:  # If there is any data for this ticker
                df2['Time'] = pd.to_datetime(df['created_at'], unit='ms')
                df2['Open'] = df["open"].astype(float)
                df2['High'] = df["high"].astype(float)
                df2['Low'] = df["low"].astype(float)
                df2['Close'] = df["close"].astype(float)
                df2['Return'] = np.log(df["close"].astype(float) / df["close"].astype(float).shift(1))
                df2["Cum_Return"] = np.exp(df2["Return"].cumsum())
                df2['Volume'] = df["volume"].astype(float)
                df2['Value'] = df["value"].astype(float)
                df2['Cum_Value'] = df2['Value'].cumsum()
                return df2
            else:
                return False
        else:
            raise ValueError(res["message"])

    def get_cum_ret_filtered_tickers(self, period: str, limit: int) -> pd.DataFrame:
        """
        Get Cumulative Return in the period in limit time for all tickers in tickers.csv

        :param period: One of ["1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", "6hour", "12hour", "1day", "3day", "1week"]
        :param limit: Number of transaction data items. Default as 100, max. value 1000
        :return: pd.DataFrame with the following columns:
            Time (datetime64[ns]): Time of the kline data
            symbol (str): Symbol of the ticker
            min_amount (float): Minimum amount of the ticker
            maker_fee_rate (float): Maker fee rate of the ticker
            taker_fee_rate (float): Taker fee rate of the ticker
            Close (float): Close price of the kline data
            Return (float): Return of the kline data
            Cum_Return (float): Cumulative return of the kline data
            Volume (float): Volume of the kline data
            Value (float): Value of the kline data
            Cum_Value (float): Cumulative value of the kline data
            period (str): Period of the kline data
            limit (int): Limit of the kline data
        """
        markets = pd.read_csv("tickers.csv")
        df = pd.DataFrame(columns=['Time', "symbol", "min_amount", "maker_fee_rate", "taker_fee_rate",
                                   'Close', 'Return', 'Cum_Return', 'Volume', 'Value', 'Cum_Value', 'period', 'limit'])
        for index, symbol in tqdm(markets.iterrows(), total=len(markets)):
            ticker = symbol['market']
            try:
                data = self.get_spot_kline(ticker, period, limit).iloc[-1]
                info = {"Time": [data["Time"]], "symbol": [ticker], "min_amount": [symbol["min_amount"]],
                        "maker_fee_rate": [symbol["maker_fee_rate"]], "taker_fee_rate": [symbol["taker_fee_rate"]],
                        "Close": [data['Close']], "Return": [data['Return']], "Cum_Return": [data["Cum_Return"]],
                        "Volume": [data['Volume']], "Value": [data["Value"]], "Cum_Value": [data['Cum_Value']],
                        "period": [period], "limit": [limit]}
                info = pd.DataFrame(info)
                if df.empty:
                    df = info.copy()
                else:
                    df = pd.concat([df, info], ignore_index=True)
            except Exception as e:
                print(e)
                continue
        return df
    
    def get_ta_filtered_tickers (self,period,limit) :
        """
        Get Technical Analysis indicators for all tickers in tickers.csv in the given period and limit

        Args:
            period (str): One of ["1min", "5min", "15min", "30min", "1hour", "2hour", "4hour", "6hour", "12hour", "1day", "3day", "1week"]
            limit (int): Number of transaction data items. Default as 100, max. value 1000

        Returns:
            pd.DataFrame: DataFrame with the following columns:
                Time (datetime64[ns]): Time of the kline data
                symbol (str): Symbol of the ticker
                min_amount (float): Minimum amount of the ticker
                maker_fee_rate (float): Maker fee rate of the ticker
                taker_fee_rate (float): Taker fee rate of the ticker
                Close (float): Close price of the kline data
                Return (float): Return of the kline data
                Cum_Return (float): Cumulative return of the kline data
                Volume (float): Volume of the kline data
                Value (float): Value of the kline data
                Cum_Value (float): Cumulative value of the kline data
                period (str): Period of the kline data
                limit (int): Limit of the kline data
                Recomandation (str): Recommendation of the technical analysis indicators
                Buy (int): Buy signal of the technical analysis indicators
                Sell (int): Sell signal of the technical analysis indicators
                Neutral (int): Neutral signal of the technical analysis indicators
        """
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
        
    def put_spot_order(self, ticker, side, order_type, amount, price, is_hide=False):
        """
        Place a Spot order

        Parameters
        ----------
        ticker : str
            Name of the ticker
        side : str
            buy/sell
        order_type : str
            limit/market/maker_only/ioc/fok
        amount : float
            Order amount
        price : float
            Order price
        is_hide : bool
            If True, it will be hidden in the public depth information

        Returns
        -------
        dict
            Result of the API call
        """
        if ticker is None or side is None or order_type is None or amount is None or price is None:
            raise ValueError("All parameters must have value")

        request_path = "/spot/order"
        data = {
            "market": ticker,
            "market_type": "SPOT",
            "side": side,
            "type": order_type,
            "amount": amount,
            "price": price,
            "client_id": "Ahad1360",
            "is_hide": is_hide,
        }
        data = json.dumps(data)
        response = self.request(
            "POST",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            data=data,
        )
        res = response.json()
        if res["code"] == 0:
            return res["data"]
        else:
            raise ValueError(res["message"])
    def modify_order (self,ticker,order_id,amount=None,price=None) :
        '''
            ticker : name of ticker
            order_id : Order ID
            amount : Order amount, which should include at least one of the two parameters, amount/price
            price :  Order price, which should include at least one of the two parameters, amount/price
        '''
        request_path = "/spot/modify-order"
        data = {
            "market": ticker,
            "market_type": "SPOT",
            "order_id": order_id,
            "amount": amount,
            "price": price,
        }
        data = json.dumps(data)
        response = self.request(
            "POST",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            data=data,
        )
        res= response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])

    def order_Status_Query(self,ticker,order_id):
        '''
            Get Status Order
        '''
        request_path = "/spot/order-status"
        data = {
            "market": ticker,
            "order_id": order_id,
        }
        data = json.dumps(data)
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            data=data,
        )
        res= response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])

    def get_unfilled_order (self,ticker,side,page=1,limit=10) :
        '''
            ticker : name of ticker
            side : buy / sell
            page : Number of pagination. Default is 1.
            limit : Number in each page. Default is 10.
        '''
        request_path = "/spot/pending-order"
        data = {
            "market": ticker,
            "market_type": "SPOT",
            "side": side,
            "client_id": "Ahad1360",
            "page": page ,
            "page" : limit ,
        }
        data = json.dumps(data)
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            data=data,
        )
        res= response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])

    def cancel_order(self,ticker,order_id):
        request_path = "/spot/cancel-order"
        data = {
            "market": ticker,
            "order_id": order_id,
        }
        data = json.dumps(data)
        response = self.request(
            "POST",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            data=data,
        )
        res= response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])
        