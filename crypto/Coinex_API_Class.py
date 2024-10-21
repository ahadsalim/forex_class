import json
import time
import hmac
import hashlib
import sqlite3
import requests
import concurrent.futures
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
        return "This class use to work with CoinEx Exchange site"
    
    def __init__(self , access_id , secret_key , connection = None , client_id=None):
        """
        Constructor for the Coinex_API class

        Args:
            access_id (str): Your CoinEx API key.
            secret_key (str): Your CoinEx API secret.
            connection (sqlite3.Connection): An open connection to a SQLite database where
                data will be stored. If None, no data will be stored.
        """
        self.access_id = access_id
        self.secret_key = secret_key
        self.conn_db    = connection
        self.client_id = client_id
        self.url = "https://api.coinex.com/v2"
        self.headers = self.HEADERS.copy()
# ************************************************ Prepare Connecting to Coinex ********************
    def gen_sign(self, method, request_path, body, timestamp):
        prepared_str = f"{method}{request_path}{body}{timestamp}"
        signature = hmac.new(bytes(self.secret_key, 'latin-1'), msg=bytes(prepared_str, 'latin-1'), digestmod=hashlib.sha256).hexdigest().lower()
        return signature

    def get_common_headers(self, signed_str, timestamp):

        headers = self.HEADERS.copy()
        headers["X-COINEX-KEY"] = self.access_id
        headers["X-COINEX-SIGN"] = signed_str
        headers["X-COINEX-TIMESTAMP"] = timestamp
        headers["Content-Type"] = "application/json; charset=utf-8"
        return headers

    def request(self, method, url, params={}, data=""):
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

            signed_str = self.gen_sign(method, request_path, body="", timestamp=timestamp)
            response = requests.get(url, params=params, headers=self.get_common_headers(signed_str, timestamp), )
        else:
            signed_str = self.gen_sign(method, request_path, body=data, timestamp=timestamp )
            response = requests.post(url, data, headers=self.get_common_headers(signed_str, timestamp))

        if response.status_code != 200:
            raise ValueError(response.text)
        return response

# ************************************************** Spot Market ********************
    def get_spot_market(self, ticker=None) : # Get all available spot market pairs or a specific one
        request_path = "/spot/market"
        params = {"market": ticker}
        response = self.request("GET", "{url}{request_path}".format(url=self.url, request_path=request_path), params=params, )
        res=response.json()
        if res["code"]==0 :
            df = pd.json_normalize(res["data"])
            return df
        else :
            raise ValueError(res["message"])

    def get_spot_price_ticker(self,ticker): # Get price of ticker in spot market in CoinEx site
        request_path = "/spot/ticker"
        params = {"market": ticker}
        response = self.request("GET", "{url}{request_path}".format(url=self.url, request_path=request_path), params=params, )
        res=response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])
    def filter_spot_market (self,min_price): # Filter tickers with price above min_price
        df = self.get_spot_market() # Get all available spot market pairs
        data = df[df['market'].str.endswith('USDT')].copy() # Select only the symbols that end with 'USDT'
        # Drop the columns that are not necessary for the analysis
        data.drop(columns=["base_ccy","base_ccy_precision","quote_ccy","quote_ccy_precision"] , inplace=True)
        # Reindex the columns to match the order of the columns in the DataFrame
        data = data.reindex(columns=['market','min_amount','maker_fee_rate','taker_fee_rate','is_amm_available','is_margin_available'])
        
        # Iterate over each symbol and update the price, value, volume_sell, volume_buy columns
        for index,symbol in tqdm(data.iterrows() ,total=len(data) , desc="Get price info From CoinEx") :
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
            # Save the DataFrame to the DB
            df.to_sql('symbols', con=self.conn_db , if_exists='replace', index=False)
            return True
        except Exception as e:
            print("Can't store data !!!" , e)
            return e

    def get_spot_kline(self, ticker, period, limit): # Get the last {limit} (Max=99) kline data of {ticker} in {period} period
        request_path = "/spot/kline"
        params = {"market": ticker, "period": period, "limit": limit}
        response = self.request("GET", "{url}{request_path}".format(url=self.url, request_path=request_path), params=params, )
        res = response.json()
        df2 = pd.DataFrame()
        if res["code"] == 0:
            df = pd.json_normalize(res["data"])
            if df.shape[0] != 0:  # If there is any data for this ticker
                df2['symbol']=ticker
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
               
                return df2.iloc[-1]
            else:
                return False
        else:
            raise ValueError(res["message"])

    def calculate_cumret_tickers(self, period: str, limit: int) : # calculate the cumulative return of all tickers in DB
        query = "SELECT * FROM symbols"
        markets = pd.read_sql_query(query, self.conn_db)

        df = pd.DataFrame(columns=['Time', "symbol", "min_amount", "maker_fee_rate", "taker_fee_rate",
                                   'Close', 'Return', 'Cum_Return', 'Volume', 'Value', 'Cum_Value', 'period', 'limit'])
        desc = "Get info From CoinEx < " + period + " > "
        for index, symbol in tqdm(markets.iterrows(), total=len(markets) ,desc=desc) :
            ticker = symbol['market']
            try:
                data = self.get_spot_kline(ticker, period, limit)
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
    '''
    def async_calculate_cumret_tickers(self, period: str, limit: int) : # calculate the cumulative return of all tickers in DB
        query = "SELECT * FROM symbols"
        markets = pd.read_sql_query(query, self.conn_db)

        df = pd.DataFrame(columns=['Time', "symbol", "min_amount", "maker_fee_rate", "taker_fee_rate",
                                   'Close', 'Return', 'Cum_Return', 'Volume', 'Value', 'Cum_Value', 'period', 'limit'])
        desc = "Get info From CoinEx < " + period + " > "
        
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=60)
        futures = []

        for index, symbol in tqdm(markets.iterrows(), total=len(markets), desc=desc):
            ticker = symbol['market']
            future = executor.submit(self.get_spot_kline, ticker, period, limit)
            futures.append(future)
            # اضافه کردن تاخیر برای رعایت محدودیت نرخ درخواست
            time.sleep(1 / 60)


        for future in futures:
            try:
                data = future.result()
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
        print(df)
        return df

            
        for index,symbol in enumerate(markets):
            data = self.get_spot_kline(symbol, period, limit)
            print(data)
            
            
        return df
    '''
    def get_ta_tickers (self,period,limit) : # Get Technical Analysis of all tickers in DB from        
        data_spot = self.calculate_cumret_tickers(period,limit)
        data_spot['Recomandation'] = None
        data_spot['Buy'] = None
        data_spot['Sell'] = None
        data_spot['Neutral'] = None
        desc = "Get info From TradingView < " + period + " > "
        for index, row in tqdm(data_spot.iterrows(), total=len(data_spot), desc=desc , position=0):
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
                print("Error for symbol : " + symbol + " ",e)
                continue
        return data_spot
    
# ****************** Portolio Management ******************
    def symbol_Candidates(self,interval, higher_interval , HMP_candles):
        """
        Find symbols with a strong buy signal on both the given interval and the higher interval.
        
        Parameters
        ----------
        interval : str
            The interval on which to select symbols. Can be '1min', '5min', '15min', '30min', '1hour', '2hour', '4hour', '1day', 
            '1week'.
        higher_interval : str
            The higher interval on which to select symbols. Must be higher than the given interval.
        HMP_candles : int
            How many previous candles? The number of candles for the given interval and the higher interval.
        
        Returns
        -------
        pd.DataFrame or str
            A DataFrame with the selected symbols, sorted by cumulative return in descending order. If no symbols are found, returns
            'empty'.
        """

        tickers_df = self.get_ta_tickers(interval, HMP_candles)
        tickers_df2 = tickers_df[(tickers_df['Recomandation'] == "STRONG_BUY")]
        higher_tickers_df = self.get_ta_tickers(higher_interval, HMP_candles)
        higher_tickers_df2 = higher_tickers_df[(higher_tickers_df['Recomandation'] == "STRONG_BUY")]
        shared_tickers_df = pd.merge(tickers_df2, higher_tickers_df2, on='symbol', how='inner')
        top_symbols = shared_tickers_df.sort_values('Cum_Return_x', ascending=False)
        if len(top_symbols) == 0 :
            return top_symbols
        else :
            top_symbols.drop(columns=['maker_fee_rate_x', 'maker_fee_rate_y', 'taker_fee_rate_y', 'Return_x', 'Return_y',
                                    'Value_x', 'Value_y','Volume_y','period_x','limit_x','Cum_Return_y', 'Cum_Value_y', 
                                    'Time_y','min_amount_y','Close_y','period_y', 'limit_y', 'Recomandation_y', 'Buy_y', 'Sell_y', 'Neutral_y'], inplace=True)
            return top_symbols

    def make_portfo(self ,num_symbols ,cash , percent_of_each_symbol , interval, higher_interval , HMP_candles , client_id) :
        """
        Make a portfolio of num_symbols symbols with the given cash and percent of each symbol, and store them in the DB.
        
        Parameters
        ----------
        num_symbols : int
            The number of symbols to include in the portfolio.
        cash : float (by USD)
            The maximum amount of cash to use for the portfolio.
        percent_of_each_symbol : float
            The percentage of the total cash to use for each symbol.
        interval : str
            The interval on which to select symbols. Can be '1min', '5min', '15min', '30min', '1hour', '2hour', '4hour', '1day', 
            '1week'.
        higher_interval : str
            The higher interval on which to select symbols. Must be higher than the given interval.
        HMP_candles : int
            How many previous candles? The number of candles for the given interval and the higher interval.
        client_id : str
            The client_id to use for SPOT Order in the portfolio.
        
        Returns
        -------
        None
        """
        self.sync_db(client_id) # sync DB with your account
        cursor = self.conn_db.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM portfo")
        result = cursor.fetchone()
        if result is None:
            num = 0
        else:
            num= result[0]
        while num < num_symbols :
            symb_df = self.symbol_Candidates(interval, higher_interval, HMP_candles)
            max_pay_symbol = cash * percent_of_each_symbol  # maximum pay for each symbol
            if len(symb_df) > 0 :            
                for index, row in symb_df.iterrows():
                    # Checking that symbols are not duplicated
                    not_duplicate = True
                    if num != 0 :
                        cursor.execute("SELECT market FROM portfo")
                        r = cursor.fetchall()
                        for tuple_item in r :
                            for symb in tuple_item:
                                if row['symbol'] == symb:
                                    print ("The symbol is repeated : ",row['symbol'])
                                    not_duplicate = False
                    if not_duplicate and num < num_symbols:
                        amount = max( int(max_pay_symbol/ float(row['Close_x'])), float(row['min_amount_x'])) # ensure minimum order support
                        amount_usdt= amount * float(row['Close_x'])
                        stat ,res= self.put_spot_order(ticker=row['symbol'], side="buy", order_type="market", amount=amount_usdt)
                        if stat ==  'done' : # if order is placed & executed
                            num += 1
                            query = "INSERT INTO portfo (amount,base_fee,ccy,client_id,created_at,discount_fee,filled_amount,filled_value,last_fill_amount,last_fill_price,maker_fee_rate,market,market_type,order_id,price,quote_fee,side,taker_fee_rate,type,unfilled_amount,updated_at,new_price) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                            try:
                                cursor.execute(query,(res['amount'],res["base_fee"],res['ccy'],res['client_id'],res['created_at'],res['discount_fee'],res['filled_amount'],res['filled_value'],res['last_fill_amount'],res['last_fill_price'],res['maker_fee_rate'],res['market'],res['market_type'],res['order_id'],res['price'],res['quote_fee'],res['side'],res['taker_fee_rate'],res['type'],res['unfilled_amount'],res['updated_at'],res['last_fill_price']))
                            except sqlite3.Error as e:
                                print("-"*60)
                                print ("Error in adding symbol to portfo in DB ! Do it manually !" , e)
                                print("-"*60)
                                self.conn_db.rollback()  # Rollback changes in case of an error
                            else:
                                self.conn_db.commit()  # Commit changes to the database
                                print ("{} added to portfo in DB".format(row["symbol"]))
                        else :
                            print("-"*60)
                            print("Error in placing order : ",stat,row['symbol'],res)
                            print("-"*60)
            else :
                print("No symbols found")
        print("The portfolio is complete !")
        
    def check_portfo(self , loss_limit, client_id):
        """
        Check if the portfolio table exists and if symbols in it have reached either the loss limit or a 10% profit.
        If a symbol has reached the loss limit, sell it and delete it from the portfolio table.
        If a symbol has reached a 10% profit, update its price in the portfolio table. 

        Parameters
        ----------
        loss_limit : float
            The percentage of the original price at which to sell a symbol if it has fallen below it.

        Returns
        -------
        bool
            True if the portfolio table exists and has been checked, False otherwise.
        """
        self.sync_db(client_id) # sync DB with your account
        cursor = self.conn_db.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM portfo")
        result = cursor.fetchone()
        if result is None:
            print("The portfo is empty.")
            return False
        else:
            query = "SELECT * FROM portfo"  
            portfo = pd.read_sql_query(query, self.conn_db)
            for index, row in portfo.iterrows():
                buy_price = float(row["last_fill_price"])
                new_price = float(row["new_price"])
                price_now=float(self.get_spot_price_ticker(row["market"])[0]["last"])
                ind= max(buy_price , new_price) * loss_limit # Calculate Loss limit price 
                if price_now <= ind :
                    # if price in under loss limit, sell it
                    stat ,res= self.put_spot_order(ticker=row['market'],side= "sell", order_type="market", amount= float(row["filled_amount"]))
                    if (stat == "done") :
                        query = "DELETE FROM portfo WHERE market = ?"
                        try:
                            cursor.execute(query, (row['market'],)) 
                        except sqlite3.Error as e:
                            print("-"*60)
                            print ("Error in deleting {} from prtfolio! But sell it. Check your DB to ensue this symbol is deleted".format(row["market"]),e)
                            print("-"*60)
                            self.conn_db.rollback()  # Rollback changes in case of an error
                        else:
                            print ("{} Sell & Deleted from prtfolio successfully !".format(row["market"]))
                            self.conn_db.commit()  # Commit changes to the database
                    else :
                        print("-"*60)
                        print("Error in placing order",stat,row['market'],res)
                        print("-"*60)
                else :
                    if price_now > new_price : # Take profit , update price to take profit
                        query = "UPDATE portfo SET new_price =? WHERE market= ?"
                        try:
                            cursor.execute(query, (price_now , row["market"]))
                        except sqlite3.Error as e:
                            print("-"*60)
                            print("Error in updating {} price !".format(row["market"]) , e)
                            print("-"*60)
                            self.conn_db.rollback()  # Rollback changes in case of an error
                        else:
                            print ("The new price of {} has now been replaced !".format(row["market"]))
                            self.conn_db.commit()  # Commit changes to the database
                    else :
                        print("The current price of {} is equal to the purchase price or less than {} of the purchase price.".format(row["market"],loss_limit))
                    
    def sync_db(self,client_id) : # sync DB with your account
        output = self.get_spot_balance()
        balance_df = pd.json_normalize(output)
        balance_df = balance_df.drop(balance_df[balance_df['ccy'] == "USDT"].index)
        balance_df['symbol'] = balance_df['ccy'].astype(str) + "USDT"
        cursor = self.conn_db.cursor()
        query = "SELECT * FROM portfo"  
        portfo_df = pd.read_sql_query(query, self.conn_db)
        # Removing symbols that no longer exist from potfo DB
        for index, row in portfo_df.iterrows():
            result = balance_df.loc[balance_df['symbol'] == row["market"]]
            if  result.empty :
                query = "DELETE FROM portfo WHERE market = ?"
                try:
                    cursor.execute(query, (row['market'],)) 
                except sqlite3.Error as e:
                    print("-"*60)
                    print ("Error in deleting symbol {} from prtfo in DB ! Do it manually !".format(row['market']) , e)
                    print("-"*60)
                    self.conn_db.rollback()  # Rollback changes in case of an error
                else:
                    self.conn_db.commit()  # Commit changes to the database
                    print ("{} deleted from portfo in DB".format(row["market"]))
            else :
                # Update amount of symbols in portfo DB for when sell some of it manualy !
                query = "UPDATE portfo SET filled_amount = ? WHERE market= ?"
                try:
                    amount= balance_df[balance_df['symbol'] == row["market"]]['available']
                    cursor.execute(query, (float(amount.iloc[0]),row['market'])) 
                except sqlite3.Error as e:
                    print("-"*60)
                    print ("Error in updating {} amount in prtfo DB ! Do it manually !".format(row['market']) , e)
                    print("-"*60)
                    self.conn_db.rollback()  # Rollback changes in case of an error
                else:
                    self.conn_db.commit()  # Commit changes to the database
                print("You own {} {} at {} per one".format(row["filled_amount"],row["market"][:-4],row['new_price']))
        # Adding symbols that exist in portfo DB
        for index, row in balance_df.iterrows():
            result = portfo_df.loc[portfo_df['market'] == row["symbol"]]
            if  result.empty :
                data= self.get_spot_price_ticker(portfo_df['market'])[0]
                query = "INSERT INTO portfo (amount,base_fee,ccy,client_id,created_at,discount_fee,filled_amount,filled_value,last_fill_amount,last_fill_price,maker_fee_rate,market,market_type,order_id,price,quote_fee,side,taker_fee_rate,type,unfilled_amount,updated_at,new_price) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                try:
                    now = str(int(time.time() * 1000))
                    cursor.execute(query, (float(row["available"])*float(data["last"]),0,row['symbol'][:-4], client_id, now, 0, row["available"] , "",row["available"],data["last"],0,row['symbol'] ,"SPOT" ,0 , "", "", "buy", 0.003,"market",0,now, data["last"]))
                except sqlite3.Error as e:
                    print("-"*60)
                    print ("Error in adding symbol to prtfo in DB ! Do it manually !" , e)
                    print("-"*60)
                    self.conn_db.rollback()  # Rollback changes in case of an error
                else:
                    self.conn_db.commit()  # Commit changes to the database
                    print ("{} added to portfo in DB".format(row["symbol"]))
        # Update amount of symbols in portfo DB for when sell some of it manualy !
    
    def put_spot_order(self, ticker, side, order_type, amount=None, price=None, is_hide=False):
        """
        Place a Spot order

        Parameters
        ----------
        ticker : str Name of the ticker
        side : str buy/sell
        order_type : str  limit/market/maker_only/ioc/fok
        amount : float Order amount by USDT (this parameter or price must have value)
        price : float Order price (this parameter or amount must have value)
        is_hide : bool If True, it will be hidden in the public depth information

        Returns
        -------
        dict
            Result of the API call
        """
        if ticker is None or side is None or order_type is None or (amount is None and price is None):
            raise ValueError("Required parameters must have value")

        request_path = "/spot/order"
        data = {
            "market": ticker,
            "market_type": "SPOT",
            "side": side,
            "type": order_type,
            "amount": amount,
            "price": price,
            "client_id": self.client_id,
            "is_hide": is_hide,
        }
        data = json.dumps(data)
        response = self.request("POST", "{url}{request_path}".format(url=self.url, request_path=request_path), data=data, )
        res = response.json()
        if res["code"] == 0:
            return "done" ,res["data"]
        else:
            return "fail" , res

    def get_spot_history(self, type_h= "trade", start_time=None, ccy=None , limit=90 , page = None):
        cursor = self.conn_db.cursor()
        cursor.execute("SELECT ltime FROM transactions ORDER BY ltime DESC LIMIT 1")
        result = cursor.fetchone()
        if result is None:
            ltime = 0
        else:
            ltime= result[0]
        if start_time == None :
            start_time = int(ltime)
        request_path = "/assets/spot/transcation-history"
        df2= pd.DataFrame()
        index =0
        params = {"type": type_h , "ccy": ccy , "limit": limit , "page": page , "start_time" : start_time }
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res=response.json()
        if res["code"]==0 :
            df=pd.json_normalize(res["data"])
            for i in range(0, len(df), 3):
                if df.iloc[i]['created_at'] > start_time :
                    df2.loc[index,'ltime']=df.iloc[i]['created_at']
                    df2.loc[index,'Time']=pd.to_datetime(df.iloc[i]['created_at'] , unit="ms")#
                    df2.loc[index,'buy']=df.iloc[i]['ccy']#
                    df2.loc[index,'amount']=df.iloc[i+1]['change']#
                    df2.loc[index,'fee']=df.iloc[i]['change']#
                    df2.loc[index,'balance']=df.iloc[i]['balance']#
                    df2.loc[index,'sold']=df.iloc[i+2]['ccy']
                    df2.loc[index,'pay']=df.iloc[i+2]['change']#
                    index+=1
            df2.to_sql('transactions', self.conn_db, if_exists='append', index=False)
        else :
            raise ValueError(res['message'])

    def calculate_profit(self) :
        cursor = self.conn_db.cursor()
        query = "SELECT * FROM transactions"  
        df = pd.read_sql_query(query, self.conn_db)
        df_b = df[df['sold']== 'USDT'].copy()
        df2=pd.DataFrame()
        i=0
        for index, row in df_b.iterrows():
            df2.loc[i,'Time_buy']= row['ltime']
            df2.loc[i,'ccy'] = row['buy']
            df2.loc[i,'pure_amount']= row['balance']
            df2.loc[i,'pay_USDT']= row['pay']
            df2.loc[i,'Time_sold']= None
            df2.loc[i,'recieve']= None
            df2.loc[i,'proft']= None 
            i +=1
        print(df_b)        
        df_s = df[df['buy']== 'USDT'].copy()
        '''
        for index, row in df_s.iterrows():
               nearest_sell = df3['ltime'].sub(int(row['ltime'])).abs().idxmin()
            df2.loc[index,'Time_sold'] = df.loc[nearest_sell,'ltime']
            df2.loc[index,'recieve'] = df.loc[nearest_sell,'balance']
            df2.loc[index,'proft'] = df2['recieve']-df2['pay_USDT']
        print(df2)
        return df2
        '''

    def modify_order (self,ticker,order_id,amount=None,price=None) :
        
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
               
    def get_spot_balance(self): # Get balance of your account
        request_path = "/assets/spot/balance"
        response = self.request("GET", "{url}{request_path}".format(url=self.url, request_path=request_path), )
        res=response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])
        
    def get_deposit_address(self,currency,chain): # Get deposit address of your account
        request_path = "/assets/deposit-address"
        params = {"ccy": currency, "chain": chain}
        response = self.request("GET", "{url}{request_path}".format(url=self.url, request_path=request_path), params=params, )
        res=response.json()
        if res["code"]==0 :
            return res["data"]
        else :
            raise ValueError(res["message"])