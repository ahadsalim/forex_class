import json
import time
import pytz
import hmac
import hashlib
import sqlite3
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
        return "This class use to work with CoinEx Exchange site"
    
    def __init__(self , access_id , secret_key , connection = None , client_id=None):
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
            print("Data stored successfully")
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
    
# ************************************************** Portolio Management ******************
    def symbol_Candidates(self,interval, higher_interval , HMP_candles):
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
        
    def check_portfo(self, loss_limit, take_profit, client_id):
        """
        Monitors and manages the portfolio by checking current prices against specified loss limits and take profit targets.
        Sells assets if the price falls below the loss limit or updates the price if it exceeds the take profit target.

        Parameters
        ----------
        loss_limit : float
            The threshold below which an asset should be sold to prevent further loss.
        take_profit : float
            The target price at which profit should be taken, updating the asset's price if exceeded.
        client_id : str
            The identifier for the client whose portfolio is being managed.

        Returns
        -------
        bool
            Returns False if the portfolio is empty, otherwise performs operations on the portfolio.
        """
        self.sync_db(client_id)  # sync DB with your account
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
                price_now = float(self.get_spot_price_ticker(row["market"])[0]["last"])
                price_proft = buy_price * take_profit
                if price_now < price_proft:
                    price_loss = max(buy_price * loss_limit, price_now * loss_limit)
                else:
                    price_loss = price_proft

                if price_now <= price_loss:
                    # if price in under loss limit, sell it
                    stat, res = self.put_spot_order(ticker=row['market'], side="sell", order_type="market", amount=float(row["filled_amount"]))
                    if (stat == "done"):
                        query = "DELETE FROM portfo WHERE market = ?"
                        try:
                            cursor.execute(query, (row['market'],))
                        except sqlite3.Error as e:
                            print("-" * 60)
                            print("Error in deleting {} from prtfolio! But sell it. Check your DB to ensue this symbol is deleted".format(row["market"]), e)
                            print("-" * 60)
                            self.conn_db.rollback()  # Rollback changes in case of an error
                        else:
                            print("{} Sell & Deleted from prtfolio successfully !".format(row["market"]))
                            self.conn_db.commit()  # Commit changes to the database
                    else:
                        print("-" * 60)
                        print("Error in placing order", stat, row['market'], res)
                        print("-" * 60)
                else:
                    if price_now > new_price:  # Take profit , update price to take profit
                        query = "UPDATE portfo SET new_price =? WHERE market= ?"
                        try:
                            cursor.execute(query, (price_now, row["market"]))
                        except sqlite3.Error as e:
                            print("-" * 60)
                            print("Error in updating {} price !".format(row["market"]), e)
                            print("-" * 60)
                            self.conn_db.rollback()  # Rollback changes in case of an error
                        else:
                            print("The new price of {} has now been replaced !".format(row["market"]))
                            self.conn_db.commit()  # Commit changes to the database
                    else:
                        print("The current price of {} is equal to the purchase price or less than {} of the purchase price.".format(row["market"], loss_limit))
                    
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
 
    def get_spot_history(self, type_h= "trade", start_time=None, end_time= None, ccy=None , limit=90 , page = None):
        '''
        This is a one time method to get all transactions.
        '''
        cursor = self.conn_db.cursor()
        cursor.execute("SELECT ltime FROM transactions ORDER BY ltime ASC LIMIT 1")
        result = cursor.fetchone()
        if result is None:
            ltime = 0
        else:
            ltime= result[0]
        
        request_path = "/assets/spot/transcation-history"
        df2= pd.DataFrame()
        index =0

        params = {"type": type_h , "ccy": ccy , "limit": limit , "page": page , "start_time" : start_time , "end_time" : end_time}
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res=response.json()
        if res["code"]==0 :
            df=pd.json_normalize(res["data"])
            df['Time'] = pd.to_datetime(df['created_at'], unit="ms")
            tehran_tz = pytz.timezone('Asia/Tehran')
            df['Time'] = df['Time'].dt.tz_localize('UTC').dt.tz_convert(tehran_tz)
            
            for i in range(0, len(df), 3):
                df2.loc[index,'ltime']=df.iloc[i]['created_at']
                df2.loc[index,'Time']=df.iloc[i]['Time']
                df2.loc[index,'buy']=df.iloc[i]['ccy']#
                df2.loc[index,'amount']=df.iloc[i+1]['change']#
                df2.loc[index,'fee']=df.iloc[i]['change']#
                df2.loc[index,'balance']=df.iloc[i]['balance']#
                df2.loc[index,'sold']=df.iloc[i+2]['ccy']
                df2.loc[index,'pay']=df.iloc[i+2]['change']#
                df2.loc[index,'flag']=0
                index+=1
            df2.to_sql('transactions', self.conn_db, if_exists='append', index=False)
            print(df2)
        else :
            raise ValueError(res['message'])
    
    def update_spot_history(self, type_h= "trade", start_time=None , limit=90):
        cursor = self.conn_db.cursor()
        cursor.execute("SELECT ltime FROM transactions ORDER BY ltime DESC LIMIT 1")
        result = cursor.fetchone()
        ltime= result[0]
        request_path = "/assets/spot/transcation-history"
        df2= pd.DataFrame()
        index =0
        params = {"type": type_h , "limit": limit }
        response = self.request(
            "GET",
            "{url}{request_path}".format(url=self.url, request_path=request_path),
            params=params,
        )
        res=response.json()
        if res["code"]==0 :
            df=pd.json_normalize(res["data"])
            df['Time'] = pd.to_datetime(df['created_at'], unit="ms")
            tehran_tz = pytz.timezone('Asia/Tehran')
            df['Time'] = df['Time'].dt.tz_localize('UTC').dt.tz_convert(tehran_tz)
            df = df[df['created_at'] > ltime]
            if not df.empty:
                for i in range(0, len(df), 3):
                    df2.loc[index,'ltime']=df.iloc[i]['created_at']
                    df2.loc[index,'Time']=df.iloc[i]['Time']
                    df2.loc[index,'buy']=df.iloc[i]['ccy']#
                    df2.loc[index,'amount']=df.iloc[i+1]['change']#
                    df2.loc[index,'fee']=df.iloc[i]['change']#
                    df2.loc[index,'balance']=df.iloc[i]['balance']#
                    df2.loc[index,'sold']=df.iloc[i+2]['ccy']
                    df2.loc[index,'pay']=df.iloc[i+2]['change']#
                    df2.loc[index,'flag']=0
                    index+=1
                df2.to_sql('transactions', self.conn_db, if_exists='append', index=False)
                print("updated transactions table.")
            else :
                print("No new transactions to update.")
        else :
            raise ValueError(res['message'])

    def update_trans_tables(self): # but_transactions & sell_transactions
        cursor = self.conn_db.cursor()
        query = "SELECT * FROM transactions WHERE flag=0 ORDER BY ltime ASC"
        df = pd.read_sql_query(query, self.conn_db)
        # update buy table
        df_buy = df[df['sold']== 'USDT'].copy() #buy
        if not df_buy.empty:
            df_buy.rename(columns={'buy':'symbol','pay':'pay_USDT','amount':'gross_symbol'}, inplace=True)
            result = []
            previous_row = None
            for _, row in df_buy.iterrows():  # پیمایش خط به خط دیتا فریم
                if previous_row is not None and row['symbol'] == previous_row['symbol']:
                    # اگر نماد مشابه است، مقادیر را جمع کن
                    result[-1]['gross_symbol'] += row['gross_symbol']
                    result[-1]['fee'] += row['fee']
                    result[-1]['fee_USDT'] += row['fee_USDT']
                    result[-1]['pay_USDT'] += row['pay_USDT']
                else:
                    result.append(row.to_dict())    # اگر نماد متفاوت است، ردیف را به لیست اضافه کن
                previous_row = row
            new_df = pd.DataFrame(result)    # تبدیل لیست به دیتا فریم جدید
            new_df.drop(columns=['sold','balance'], inplace=True) # drop
            new_df['net_symbol'] = new_df['gross_symbol'] + new_df['fee']
            order=['ltime','Time','symbol','gross_symbol','fee','net_symbol','fee_USDT','pay_USDT','flag']
            new_df = new_df[order]
            new_df.to_sql('buy_transactions',self.conn_db,if_exists='append', index=False)
            query = "UPDATE transactions SET flag=1 WHERE sold='USDT'"
            cursor.execute(query)
            print("buy_Transctions table is updated and all flag of buy record in transactions table changed to 1")
        
        #update sell table
        df_sell = df[df['buy']== 'USDT'].copy()  # sell symbol
        df_sell.rename(columns={'sold':'symbol','pay':'pay_symbol','amount':'gross_USDT'}, inplace=True)
        df_sell.drop(columns={'buy','fee','balance'} , inplace=True)
        if not df_sell.empty:
            result = []
            previous_row = None
            for _, row in df_sell.iterrows():  # پیمایش خط به خط دیتا فریم
                if previous_row is not None and row['symbol'] == previous_row['symbol']:
                    # اگر نماد مشابه است، مقادیر را جمع کن
                    result[-1]['gross_USDT'] += row['gross_USDT']
                    result[-1]['fee_USDT'] += row['fee_USDT']
                    result[-1]['pay_symbol'] += row['pay_symbol']
                else:
                    result.append(row.to_dict())    # اگر نماد متفاوت است، ردیف را به لیست اضافه کن
                previous_row = row
            new_df = pd.DataFrame(result)    # تبدیل لیست به دیتا فریم جدید
            new_df['net_USDT'] = new_df['gross_USDT'] + new_df['fee_USDT']
            order=['ltime','Time','symbol','gross_USDT','fee_USDT','net_USDT','pay_symbol','flag']
            new_df = new_df[order]
            new_df.to_sql('sell_transactions',self.conn_db,if_exists='append', index=False)
            query = "UPDATE transactions SET flag=1 WHERE buy='USDT'"
            cursor.execute(query)
            print("sell_Transctions table is updated and all flag of sell record in transactions table changed to 1")

    def create_profit_db(self):
        cursor = self.conn_db.cursor()
        query = "SELECT * FROM buy_transactions WHERE flag=0 ORDER BY ltime ASC"
        b_df = pd.read_sql_query(query, self.conn_db)
        for i,row_b in b_df.iterrows() :
            symbol=row_b['symbol']
            query= f"SELECT * FROM sell_transactions WHERE symbol='{symbol}' AND flag=0"
            df_symbol = pd.read_sql_query(query,self.conn_db)
            if df_symbol.empty:
                print(f"{symbol} doesn't sell yet !")
            else :
                row_s = df_symbol.iloc[0]
                remain=row_b['net_symbol'] + row_s['pay_symbol'] # pay_symbol is a negetive number !
                if remain == 0 :
                    query = '''INSERT INTO profit 
                    (b_rowid,b_ltime,symbol,gross_symbol,b_fee_usdt,net_symbol,pay_usdt,pay_symbol,s_rowid,s_ltime,gross_usdt,s_fee_usdt,net_usdt,remain) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                    try:
                        cursor.execute(query, (row_b['rowid'],row_b['ltime'],symbol,row_b['gross_symbol'], row_b['fee_USDT'], row_b['net_symbol'], 
                                               row_b['pay_USDT'],row_s['pay_symbol'],int(row_s['rowid']),row_s['ltime'],row_s['gross_USDT'],
                                               row_s['fee_USDT'], row_s['net_USDT'],remain))
                        q_b="UPDATE buy_transactions SET flag=1 WHERE rowid=?"
                        cursor.execute(q_b, (row_b['rowid'],))
                        q_s="UPDATE sell_transactions SET flag=1 WHERE rowid=?"
                        cursor.execute(q_s, (int(row_s['rowid']),))
                        self.conn_db.commit()
                        print(f"Updated profit record for {symbol} at index {row_b['rowid']} in buy_transactions and at {row_s['rowid']} in sell_transactions.")
                    except sqlite3.Error as e:
                        print(f"An error occurred: {e}")
                        self.conn_db.rollback()
                elif remain > 0 :
                    query = '''INSERT INTO profit 
                    (b_rowid,b_ltime,symbol,gross_symbol,b_fee_usdt,net_symbol,pay_usdt,pay_symbol,s_rowid,s_ltime,gross_usdt,s_fee_usdt,net_usdt,remain) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                    
                    try:
                        cursor.execute(query, (row_b['rowid'],row_b['ltime'],symbol,row_b['gross_symbol'], row_b['fee_USDT'], row_b['net_symbol'], 
                                               row_b['pay_USDT'],row_s['pay_symbol'],int(row_s['rowid']),row_s['ltime'],row_s['gross_USDT'],
                                               row_s['fee_USDT'], row_s['net_USDT'],remain))
                        q_b="UPDATE buy_transactions SET flag=1 WHERE rowid=?"
                        cursor.execute(q_b, (row_b['rowid'],))
                        q_s="UPDATE sell_transactions SET flag=1 WHERE rowid=?"
                        cursor.execute(q_s, (int(row_s['rowid']),))
                        self.conn_db.commit()
                        print(f"Updated profit record for {symbol} at index {row_b['rowid']} in buy_transactions and at {row_s['rowid']} in sell_transactions.")
                    except sqlite3.Error as e:
                        print(f"An error occurred: {e}")
                        self.conn_db.rollback()
                else : # some symbols are completely sold out (with fees !!!)
                    remain1=row_b['gross_symbol'] + row_s['pay_symbol'] # pay_symbol is a negetive number !
                    if remain1 == 0 :
                        query = '''INSERT INTO profit 
                        (b_rowid,b_ltime,symbol,gross_symbol,b_fee_usdt,net_symbol,pay_usdt,pay_symbol,s_rowid,s_ltime,gross_usdt,s_fee_usdt,net_usdt,remain) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                        try:
                            cursor.execute(query, (row_b['rowid'],row_b['ltime'],symbol,row_b['gross_symbol'], row_b['fee_USDT'], row_b['net_symbol'], 
                                                row_b['pay_USDT'],row_s['pay_symbol'],int(row_s['rowid']),row_s['ltime'],row_s['gross_USDT'],
                                                row_s['fee_USDT'], row_s['net_USDT'],remain))
                            q_b="UPDATE buy_transactions SET flag=1 WHERE rowid=?"
                            cursor.execute(q_b, (row_b['rowid'],))
                            q_s="UPDATE sell_transactions SET flag=1 WHERE rowid=?"
                            cursor.execute(q_s, (int(row_s['rowid']),))
                            self.conn_db.commit()
                            print(f"Updated profit record for {symbol} at index {row_b['rowid']} in buy_transactions and at {row_s['rowid']} in sell_transactions.")
                        except sqlite3.Error as e:
                            print(f"An error occurred: {e}")
                            self.conn_db.rollback()
                    elif remain1 > 0 :
                        query = '''INSERT INTO profit 
                        (b_rowid,b_ltime,symbol,gross_symbol,b_fee_usdt,net_symbol,pay_usdt,pay_symbol,s_rowid,s_ltime,gross_usdt,s_fee_usdt,net_usdt,remain) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                        
                        try:
                            cursor.execute(query, (row_b['rowid'],row_b['ltime'],symbol,row_b['gross_symbol'], row_b['fee_USDT'], row_b['net_symbol'], 
                                                row_b['pay_USDT'],row_s['pay_symbol'],int(row_s['rowid']),row_s['ltime'],row_s['gross_USDT'],
                                                row_s['fee_USDT'], row_s['net_USDT'],remain))
                            q_b="UPDATE buy_transactions SET flag=1 WHERE rowid=?"
                            cursor.execute(q_b, (row_b['rowid'],))
                            q_s="UPDATE sell_transactions SET flag=1 WHERE rowid=?"
                            cursor.execute(q_s, (int(row_s['rowid']),))
                            self.conn_db.commit()
                            print(f"Updated profit record for {symbol} at index {row_b['rowid']} in buy_transactions and at {row_s['rowid']} in sell_transactions.")
                        except sqlite3.Error as e:
                            print(f"An error occurred: {e}")
                            self.conn_db.rollback()
                    ###############################
                query= "SELECT * FROM sell_transactions WHERE flag=0"
                df = pd.read_sql_query(query,self.conn_db)
                if not df.empty:
                    for _,row_s in df.iterrows() :
                        symbol=row_s['symbol'] # یک فروش که به چند خرید وصل است !
                        query= f"SELECT * FROM buy_transactions WHERE symbol='{symbol}' AND flag=0"
                        df_symbol = pd.read_sql_query(query,self.conn_db)
                        if df_symbol.empty:
                            print(f"No buy transaction found for {symbol}, maybe you deposits to the account.")
                            q_s="UPDATE sell_transactions SET flag=2 WHERE rowid=?"
                            cursor.execute(q_s, (int(row_s['rowid']),))
                            self.conn_db.commit()
                        else :
                            for _,row_b in df_symbol.iterrows() :
                                if row_b['gross_symbol'] + row_s['pay_symbol'] == 0 :
                                    query = '''INSERT INTO profit 
                                    (b_rowid,b_ltime,symbol,gross_symbol,b_fee_usdt,net_symbol,pay_usdt,pay_symbol,s_rowid,s_ltime,gross_usdt,s_fee_usdt,net_usdt,remain) 
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                                    try:
                                        cursor.execute(query, (row_b['rowid'],row_b['ltime'],symbol,row_b['gross_symbol'], row_b['fee_USDT'], row_b['net_symbol'], 
                                                            row_b['pay_USDT'],row_s['pay_symbol'],int(row_s['rowid']),row_s['ltime'],row_s['gross_USDT'],
                                                            row_s['fee_USDT'], row_s['net_USDT'],row_b['gross_symbol'] + row_s['pay_symbol']))
                                        q_b="UPDATE buy_transactions SET flag=1 WHERE rowid=?"
                                        cursor.execute(q_b, (row_b['rowid'],))
                                        q_s="UPDATE sell_transactions SET flag=1 WHERE rowid=?"
                                        cursor.execute(q_s, (int(row_s['rowid']),))
                                        self.conn_db.commit()
                                        print(f"Updated profit record for {symbol} at index {row_b['rowid']} in buy_transactions and at {row_s['rowid']} in sell_transactions.")
                                    except sqlite3.Error as e:
                                        print(f"An error occurred: {e}")
                                        self.conn_db.rollback()

        # for rows with remain < 0 :
        query_remain = "SELECT * FROM profit WHERE remain > 0 "
        df = pd.read_sql_query(query_remain, self.conn_db)
        for i,row in df.iterrows() :
            symbol=row['symbol']
            query= f"SELECT * FROM sell_transactions WHERE symbol='{symbol}' AND flag = 0"
            df_symbol = pd.read_sql_query(query,self.conn_db)
            if not df_symbol.empty:
                row_s = df_symbol.iloc[0]
                remain=row['remain'] + row_s['pay_symbol'] # pay_symbol is a negetive number !
                if row['remain'] > abs(row_s['pay_symbol']) :
                    query = '''INSERT INTO profit 
                    (b_rowid,b_ltime,symbol,net_symbol,pay_symbol,s_rowid,s_ltime,gross_usdt,s_fee_usdt,net_usdt,remain) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
                    try:
                        cursor.execute(query, (row_b['rowid'],row_b['ltime'],symbol, row['remain'], row_s['pay_symbol'],int(row_s['rowid']),row_s['ltime'],
                                               row_s['gross_USDT'],row_s['fee_USDT'], row_s['net_USDT'],remain))
                        q_s="UPDATE sell_transactions SET flag=1 WHERE rowid=?"
                        cursor.execute(q_s, (int(row_s['rowid']),))
                        q_s="UPDATE profit SET remain=? WHERE b_rowid=?"
                        cursor.execute(q_s, (remain,int(row['b_rowid']),))
                        self.conn_db.commit()
                        print(f"Updated profit record for {symbol} at {row_s['rowid']} in sell_transactions.")
                    except sqlite3.Error as e:
                        print(f"An error occurred: {e}")
                        self.conn_db.rollback()
        query = "SELECT * FROM profit"
        df = pd.read_sql_query(query, self.conn_db)
        df['profit'] = df['pay_usdt'] + df['net_usdt']
        df.to_sql("profit", self.conn_db, if_exists="replace" , index=False)
        print("Profit table updated with profit column successfully.")

    def calculate_profit(self):
        query = "SELECT * FROM profit ORDER BY b_ltime"
        df = pd.read_sql_query(query, self.conn_db)
        df['cum_proftit'] = df['profit'].cumsum()
        print(df)
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