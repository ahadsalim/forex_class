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
        self.username = username
        self.password = password
        self.server = exchange_server
        self.conn_db = connection

# ************************************************** Spot Market ********************
    def initialize(self):
        if not mt5.initialize(login=self.username, password=self.password, server=self.server):
            print("initialize() failed, error code =", mt5.last_error())
            quit()
    
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

    def make_portfo(self ,num_symbols ,interval, higher_interval , HMP_candles , lot=0.01 , stop_loss=25, take_profit=50, deviation =20 ) :
        #self.sync_db() # sync DB with your account
        cursor = self.conn_db.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM portfo")
        result = cursor.fetchone()
        if result is None:
            num = 0
        else:
            num= result[0]
        while num < num_symbols :
            symb_df = self.symbol_Candidates(interval, higher_interval, HMP_candles)
            if len(symb_df) > 0 :            
                for index, row in symb_df.iterrows():
                    # Checking that symbols are not duplicated
                    not_duplicate = True
                    if num != 0 :
                        cursor.execute("SELECT symbol FROM portfo")
                        r = cursor.fetchall()
                        for tuple_item in r :
                            for symb in tuple_item:
                                if row['Symbol'] == symb:
                                    print ("The symbol is repeated : ",row['Symbol'])
                                    not_duplicate = False
                    if not_duplicate and num < num_symbols:
                        if row['Cum_Return_x'] >1 and row['Cum_Return_y'] > 1 : ###########check tradingview status ?!!!
                            o_t = "buy"
                        elif row['Cum_Return_x'] <1 and row['Cum_Return_y'] < 1 :
                            o_t = "sell"
                        else :
                            continue
                        print(row['Symbol'], row['Cum_Return_x'], row['Cum_Return_y'])#################
                        if row['Category_x'] != "Forex" :
                            lot = round(lot, 1)
                            lot = max(lot, 0.1)

                        print("-"*60)
                        stat,res = self.put_order(symbol=row['Symbol'], order_type=o_t, lot= lot , stop_loss=stop_loss , take_profit=take_profit , deviation=deviation)
                        print(res)
                        if stat ==  'done' : # if order is placed & executed
                            num += 1
                            query = "INSERT INTO portfo VALUES (?,?,?,?,?,?,?)"
                            try:
                                cursor.execute(query,(res.order,res.volume,res.price,row['Symbol'],res.request.sl,res.request.tp,o_t))
                            except sqlite3.Error as e:
                                print("-"*60)
                                print ("Error in adding symbol to portfo in DB ! Do it manually !" , e)
                                print("-"*60)
                                self.conn_db.rollback()  # Rollback changes in case of an error
                            else:
                                self.conn_db.commit()  # Commit changes to the database
                                print ("{} added to portfo in DB".format(row["Symbol"]))
                        else :
                            print("-"*60)
                            print("Error in placing order : ",stat,row['Symbol'])
                            print("-"*60)
            else :
                print("No symbols found")
        print("The portfolio is complete !")
        
    def check_portfo(self,stop_loss,take_profit):
        #self.sync_db() # sync DB with your account
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
                buy_price = float(row["price"])
                point = mt5.symbol_info(row['symbol']).point
                if row['order'] == "buy":
                    now_price = mt5.symbol_info_tick(row['symbol']).bid
                    s_l = now_price - stop_loss * point
                    t_p = max(now_price + take_profit * point,row["tp"])
                elif row['order'] == "sell":
                    now_price = mt5.symbol_info_tick(row['symbol']).ask
                    s_l = now_price + stop_loss * point
                    t_p = min(now_price - take_profit * point , row["sl"])
                
                if now_price >= buy_price and row['order'] == "buy" :
                    stat ,res= self.modify_order(order_id=row['order_id'] ,symbol=row['Symbol'] ,order_type=row['order'],stop_loss=s_l,take_profit=t_p)
                elif now_price <= buy_price and row['order'] == "sell" :
                    stat ,res= self.modify_order(order_id=row['order_id'] ,symbol=row['Symbol'] ,order_type=row['order'],stop_loss=s_l,take_profit=t_p)
                else :
                    continue # do nothing !
                if (stat == "done") :
                    query = "UPDATE portfo SET sl=? , tp=? VALUE () WHERE order_id = ?"
                    try:
                        cursor.execute(query, (s_l,t_p,row['order_id'],)) 
                    except sqlite3.Error as e:
                        print("-"*60)
                        print ("Error in updating order {} from prtfolio!".format(row["symbol"]),e)
                        print("-"*60)
                    else:
                        print ("{} updated ! ".format(row["symbol"]))
                        self.conn_db.commit()  # Commit changes to the database
                else :
                    print("-"*60)
                    print("Error in placing order",stat,row['symbol'],res)
                    print("-"*60)
                    
    def sync_db(self) : # sync DB with your account
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
                    cursor.execute(query, (float(row["available"])*float(data["last"]),0,row['symbol'][:-4], now, 0, row["available"] , "",row["available"],data["last"],0,row['symbol'] ,"SPOT" ,0 , "", "", "buy", 0.003,"market",0,now, data["last"]))
                except sqlite3.Error as e:
                    print("-"*60)
                    print ("Error in adding symbol to prtfo in DB ! Do it manually !" , e)
                    print("-"*60)
                    self.conn_db.rollback()  # Rollback changes in case of an error
                else:
                    self.conn_db.commit()  # Commit changes to the database
                    print ("{} added to portfo in DB".format(row["symbol"]))
        # Update amount of symbols in portfo DB for when sell some of it manualy !
    
    def put_order(self, symbol: str, order_type: str, lot: float , stop_loss: int, take_profit: int, deviation : int ):
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
        print(order_type,lot)############################
        if order_type == "buy":
            o_t = mt5.ORDER_TYPE_BUY
            s_l = price - stop_loss * point
            t_p = price + take_profit * point
        elif order_type == "sell":
            o_t = mt5.ORDER_TYPE_SELL
            s_l = price + stop_loss * point
            t_p = price - take_profit * point
        elif order_type == "buy_limit":
            o_t = mt5.ORDER_TYPE_BUY_LIMIT
        elif order_type == "sell_limit":
            o_t = mt5.ORDER_TYPE_SELL_LIMIT
        elif order_type == "buy_stop":
            o_t = mt5.ORDER_TYPE_BUY_STOP
        elif order_type == "sell_stop":
            o_t = mt5.ORDER_TYPE_SELL_STOP
        elif order_type == "buy_stop_limit":
            o_t = mt5.ORDER_TYPE_BUY_STOP_LIMIT
        elif order_type == "sell_stop_limit":
            o_t = mt5.ORDER_TYPE_SELL_STOP_LIMIT
        elif order_type == "close":
            o_t = mt5.ORDER_TYPE_CLOSE_BY 
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": o_t,
            "price": price,
            "sl": s_l,
            "tp": t_p,
            "deviation": deviation,
            "magic": 4919,
            "comment": "python script open",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        result = mt5.order_send(request)
        print("1. Order send : {} {} {} lots at {} with deviation={} points".format(order_type,symbol, lot, price, deviation))
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print("2. Order send failed, retcode={}".format(result.retcode))
            result_dict = result._asdict()
            for field in result_dict.keys():
                print("   {}={}".format(field, result_dict[field]))
                if field == "request":
                    traderequest_dict = result_dict[field]._asdict()
                    for tradereq_filed in traderequest_dict:
                        print("       traderequest: {}={}".format(tradereq_filed, traderequest_dict[tradereq_filed]))
            return "failed" , result

        print("2. Order send Done, opened position with POSITION_TICKET={}".format(result.order))
        return "done", result

    def modify_order (self, order_id : int, symbol: str, order_type: str, stop_loss: int, take_profit: int ):
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            print(symbol, "not found, can not call order_check()")
            return None

        if not symbol_info.visible:
            print(symbol, "is not visible, trying to switch on")
            if not mt5.symbol_select(symbol, True):
                print("symbol_select({}}) failed, exit", symbol)
                return None

        request = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "position": order_id,
            "symbol": symbol,
            "sl": stop_loss,
            "tp": take_profit,
            "comment": "python script Modified order",
        }
        result = mt5.order_send(request)
        print("1. Send Modifing Order {} for {} ".format(order_id,symbol))
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print("2. Order send failed, retcode={}".format(result.retcode))
            result_dict = result._asdict()
            for field in result_dict.keys():
                print("   {}={}".format(field, result_dict[field]))
                if field == "request":
                    traderequest_dict = result_dict[field]._asdict()
                    for tradereq_filed in traderequest_dict:
                        print("       traderequest: {}={}".format(tradereq_filed, traderequest_dict[tradereq_filed]))
            return "failed" , result

        print("2. Order Modified, Stop Loss and Take Profit updated. POSITION_TICKET={}".format(result.order))
        return "done", result
        
        
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