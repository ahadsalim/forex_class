import json
import time
import sqlite3
import MetaTrader5 as mt5
from tqdm import tqdm
import pandas as pd
import numpy as np
from tradingview_ta import TA_Handler, Interval, Exchange , TradingView

class MT5_API(object):
    
    def __repr__(self): 
        return "This class use to work with MetaTrader 5"
    
    def __init__(self, username, password, exchange_server, connection=None):
        """
        Initialize the MT5_API class with the given parameters.

        Parameters
        ----------
        username : str
            The username for the MT5 account.
        password : str
            The password for the MT5 account.
        exchange_server : str
            The server to connect to for trading.
        connection : sqlite3.Connection, optional
            The SQLite database connection to store portfolio information. If not provided, a new connection will be created.

        Returns
        -------
        None
        """
        self.username = username
        self.password = password
        self.server = exchange_server
        self.conn_db = connection

# ************************************************** Spot Market ********************
    def initialize(self):
        if not mt5.initialize(login=self.username, password=self.password, server=self.server):
            print("initialize() failed, error code =", mt5.last_error())
            quit()
        #print(mt5.terminal_info())
        #print(mt5.version())
    
    def shutdown(self):
        mt5.shutdown()

    def priod_to_text (self,period) :
        if period == 1 : return "1m"
        if period == 2 : return "M2"
        if period == 3 : return "M3"
        if period == 4 : return "M4"
        if period == 5 : return "5m"
        if period == 6 : return "M6"
        if period == 10 : return "M10"
        if period == 12 : return "M12"
        if period == 15 : return "15m"
        if period == 20 : return "M20"
        if period == 30 : return "30m"
        if period == 16385 : return "1h"
        if period == 16386 : return "2h"
        if period == 16387 : return "H3"
        if period == 16388 : return "4h"
        if period == 16390 : return "H6"
        if period == 16392 : return "H8"
        if period == 16396 : return "H12"
        if period == 16408 : return "1d"
        if period == 32769 : return "1W"
        if period == 49153 : return "1M"

    def get_list_symbols(self) : # Get all available symbol
        symbols = mt5.symbols_get()
        sy=[]
        for s in symbols:
            name= s.name
            category= s.path[:s.path.index("\\")]
            sy.append([name,category])
        return sy

    def get_return_symbol(self,symbol,timeframe , limit): # Get the last {limit} (Max=99) kline data of {ticker} in {period} period
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, limit)
        if rates is None:
            return (f"Error getting rates for {symbol}")
        rates_df = pd.DataFrame(rates)
        rates_df["datetime"] = pd.to_datetime(rates_df['time'], unit='s')
        rates_df["returns"] = np.log(rates_df["close"] / rates_df["close"].shift(1))
        rates_df["cum_return"] = np.exp(rates_df["returns"].cumsum())
        rates_df['length'] = rates_df["close"] - rates_df["open"]
        rates_df["mean_length"] = rates_df["length"].rolling(window=limit).mean()
        return rates_df.iloc[-1]

    def calculate_cumret_symbols(self, period : int, limit: int) : # calculate the cumulative return of all symbols
        symbols= self.get_list_symbols()
        df = pd.DataFrame(columns=['Time', "symbol", 'Close', 'Cum_Return', 'mean_lengh','Volume', 'Spread', 'period', 'limit'])
        desc = "Get data From MetaTrader < "+ self.priod_to_text(period) +" > "
        for symbol,category in tqdm(symbols, total=len(symbols) ,desc=desc) :
            try:
                data = self.get_return_symbol(symbol, period, limit)
                info = {"Time": [data["datetime"]], "Symbol": [symbol], "Category":[category], "Close": [data['close']], "Cum_Return": [data["cum_return"]],
                        "mean_lengh": [data["mean_length"]] ,"Volume": [data['tick_volume']], "Spread": [data["spread"]],"Period": [period], "Limit": [limit]}
                info = pd.DataFrame(info)
                if df.empty:
                    df = info.copy()
                else:
                    df = pd.concat([df, info], ignore_index=True)
            except Exception as e:
                print(e)
                continue
        return df
    
    def get_TA_symbols (self,period,limit) : # Get Technical Analysis of all symbols from TradingView        
        if period == 1 or period == 5 or period == 15 or period == 30 or period == 16385 or period == 16386 or period == 16388 or period == 16408 or period == 32769 or period == 49153 :
            data_spot = self.calculate_cumret_symbols(period,limit)
            data_spot['Recomandation'] = None
            data_spot['Buy'] = None
            data_spot['Sell'] = None
            data_spot['Neutral'] = None
            desc = "Get info From TradingView < " + self.priod_to_text(period) + " > "
            for index, row in tqdm(data_spot.iterrows(), total=len(data_spot), desc=desc , position=0):
                symbol=row['Symbol']
                if row['Category'] == 'Forex':
                    try : 
                        data= (TA_Handler(symbol=symbol, screener='forex', exchange="FX_IDC", interval=self.priod_to_text(period))).get_analysis().summary
                        data_spot.loc[index, 'Recomandation'] =data['RECOMMENDATION']
                        data_spot.loc[index, 'Buy'] = data['BUY']
                        data_spot.loc[index, 'Sell'] = data['SELL']
                        data_spot.loc[index, 'Neutral'] = data['NEUTRAL']
                    except Exception as e:
                        print("Error for symbol : " + symbol + " ",e)
                        continue
            return data_spot.sort_values(by='Cum_Return', ascending=False)
        else :
            print("This time frame is not supported in TradingView.")
            
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

        symbols_df = self.get_TA_symbols(interval, HMP_candles)
        filterd_df = pd.DataFrame(columns=symbols_df.columns)
        filterd_df = pd.concat([symbols_df.iloc[:3], symbols_df.iloc[-3:]], ignore_index=True)
        
        higher_symbols_df = self.get_TA_symbols(higher_interval, HMP_candles)
        filterd_df2 = pd.DataFrame(columns=higher_symbols_df.columns)
        filterd_df2 = pd.concat([higher_symbols_df.iloc[:3], higher_symbols_df.iloc[-3:]], ignore_index=True)

        shared_tickers_df = pd.merge(filterd_df, filterd_df2, on='Symbol', how='inner')
        if len(shared_tickers_df) == 0 :
            return shared_tickers_df
        else :
            shared_tickers_df.drop(columns=['Time_y', 'Category_y', 'Close_y', 'Time_y','Volume_y', 'Spread_y','Limit_y','mean_lengh_y'], inplace=True)
            return shared_tickers_df

    def make_portfo(self ,num_symbols ,cash , percent_of_each_symbol , interval, higher_interval , HMP_candles) :
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
        self.sync_db() # sync DB with your account
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
                        cursor.execute("SELECT symbol FROM portfo")
                        r = cursor.fetchall()
                        for tuple_item in r :
                            for symb in tuple_item:
                                if row['symbol'] == symb:
                                    print ("The symbol is repeated : ",row['symbol'])
                                    not_duplicate = False
                    if not_duplicate and num < num_symbols:
                        stat = self.put_order(symbol=row['symbol'], order_type="buy", stop_loss=50 , take_profit=50 , lot=0.01 , deviation=20)
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
    
    def put_order(self, symbol: str, order_type: str, stop_loss: int, take_profit: int, lot=0.01, deviation=20 ):
        """
        Places an order for a given symbol with specified parameters.

        Parameters
        ----------
        symbol : str
            The trading symbol for which the order is to be placed.
        order_type : str
            The type of order to be placed. Can be 'buy', 'sell', 'buy_limit', 'sell_limit', 'buy_stop', 'sell_stop', 
            'buy_stop_limit', 'sell_stop_limit', or 'close'.
        stop_loss : int
            The stop loss value in points.
        take_profit : int
            The take profit value in points.
        lot : float, optional
            The volume of the order in lots. Default is 0.01.
        deviation : int, optional
            The maximum price deviation in points. Default is 20.

        Returns
        -------
        str
            Returns "done" if the order is successfully placed and executed, otherwise returns "failed".
        """
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            print(symbol, "not found, can not call order_check()")
            return None

        if not symbol_info.visible:
            print(symbol, "is not visible, trying to switch on")
            if not mt5.symbol_select(symbol, True):
                print("symbol_select({}}) failed, exit", symbol)
                return None

        point = mt5.symbol_info(symbol).point
        price = mt5.symbol_info_tick(symbol).ask
        if order_type == "buy":
            order_type = mt5.ORDER_TYPE_BUY
        elif order_type == "sell":
            order_type = mt5.ORDER_TYPE_SELL
        elif order_type == "buy_limit":
            order_type = mt5.ORDER_TYPE_BUY_LIMIT
        elif order_type == "sell_limit":
            order_type = mt5.ORDER_TYPE_SELL_LIMIT
        elif order_type == "buy_stop":
            order_type = mt5.ORDER_TYPE_BUY_STOP
        elif order_type == "sell_stop":
            order_type = mt5.ORDER_TYPE_SELL_STOP
        elif order_type == "buy_stop_limit":
            order_type = mt5.ORDER_TYPE_BUY_STOP_LIMIT
        elif order_type == "sell_stop_limit":
            order_type = mt5.ORDER_TYPE_SELL_STOP_LIMIT
        elif order_type == "close":
            order_type = mt5.ORDER_TYPE_CLOSE_BY 
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": order_type,
            "price": price,
            "sl": price - stop_loss * point,
            "tp": price + take_profit * point,
            "deviation": deviation,
            "magic": 4919,
            "comment": "python script open",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }
        result = mt5.order_send(request)
        print("1. order_send(): by {} {} lots at {} with deviation={} points".format(symbol, lot, price, deviation))
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print("2. order_send failed, retcode={}".format(result.retcode))
            result_dict = result._asdict()
            for field in result_dict.keys():
                print("   {}={}".format(field, result_dict[field]))
                if field == "request":
                    traderequest_dict = result_dict[field]._asdict()
                    for tradereq_filed in traderequest_dict:
                        print("       traderequest: {}={}".format(tradereq_filed, traderequest_dict[tradereq_filed]))
            print("shutdown() and quit")
            return "failed"

        print("2. order_send done, ", result)
        print("   opened position with POSITION_TICKET={}".format(result.order))
        print("   sleep 2 seconds before closing position #{}".format(result.order))
        return "done"


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