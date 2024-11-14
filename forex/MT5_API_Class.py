import json
import time
import MetaTrader5 as mt5
from tqdm import tqdm
import pandas as pd
import numpy as np
from tradingview_ta import TA_Handler, Interval, Exchange , TradingView

class MT5_API(object):
    
    def __repr__(self): 
        return "This class use to work with MetaTrader 5"
    
    def __init__(self, username, password, exchange_server):
        self.username = username
        self.password = password
        self.server = exchange_server

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
        pos_total=mt5.positions_total()
        if pos_total > 0 :
            print("Total open positions=",pos_total)
        else:
            print("The portfo is empty !")
        
        while pos_total < num_symbols :
            symb_df = self.symbol_Candidates(interval, higher_interval, HMP_candles)
            if len(symb_df) > 0 :            
                for index, row in symb_df.iterrows():
                    # Checking that symbols are not duplicated
                    positions=mt5.positions_get(symbol=row['Symbol'])
                    if positions == () :
                        print("No positions on {} , Let's trade it.".format(row['Symbol']))
                        if pos_total < num_symbols:
                            if row['Cum_Return_x'] >1 and row['Cum_Return_y'] > 1 : ###########check tradingview status ?!!!
                                o_t = "buy"
                            elif row['Cum_Return_x'] <1 and row['Cum_Return_y'] < 1 :
                                o_t = "sell"
                            else :
                                continue
                            if row['Category_x'] != "Forex" :
                                lot = round(lot, 1)
                                lot = max(lot, 0.1)

                            print("-"*60)
                            stat,res = self.put_order(symbol=row['Symbol'], order_type=o_t, lot= lot , stop_loss=stop_loss , take_profit=take_profit , deviation=deviation)
                            print(res)
                            if stat ==  'done' : # if order is placed & executed
                                pos_total += 1
                            else :
                                print("-"*60)
                                print("Error in placing order : ",stat,row['Symbol'])
                                print("-"*60)

                    elif len(positions)>0:
                        print("The symbol is repeated : ",row['Symbol'])
            else :
                print("No symbols found")
        print("The portfolio is complete !")
        
    def check_portfo(self,num_symbols ,interval, higher_interval , HMP_candles , lot=0.01 , stop_loss=25, take_profit=50, deviation =2):
        portfo = mt5.positions_get()
        if portfo ==() :
            print("The portfo is empty !")
        else:
            portfo_df=pd.DataFrame(list(portfo),columns=portfo[0]._asdict().keys())
            portfo_df.drop(['time_msc','time_update','time_update_msc','reason','comment','external_id'], axis=1, inplace=True)
            print("Checking the portfolio...")
            for index, row in portfo_df.iterrows():
                point = mt5.symbol_info(row['symbol']).point
                change=False
                if row['type'] == "0" and row['price_current'] > row['price_open']:  # update sl & tp on buy order
                    s_l = row['price_current'] - stop_loss * point
                    t_p = row['price_current'] + take_profit * point
                    change=True
                elif row['type'] == "1" and row['price_current'] < row['price_open']: # update sl & tp on sell order
                    s_l = row['price_current'] + stop_loss * point
                    t_p = row['price_current'] - take_profit * point
                    change=True
                if change :
                    stat ,res= self.modify_order(order_id=row['ticket'] ,symbol=row['symbol'] ,order_type=row['type'],stop_loss=s_l,take_profit=t_p)
                else :
                    continue # do nothing !
                if (stat == "done") :
                    print ("{} updated ! ".format(row["symbol"]))
                else :
                    print("-"*60)
                    print("Error in placing order",stat,row['symbol'],res)
                    print("-"*60)
          
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
        price_a = mt5.symbol_info_tick(symbol).ask
        price_b = mt5.symbol_info_tick(symbol).bid
        print("point=",point , "ask=",price_a , "bid=", price_b)     ############################
        if order_type == "buy":
            o_t = mt5.ORDER_TYPE_BUY
            s_l = price_b - stop_loss * point
            t_p = price_a + take_profit * point
            price=price_a
        elif order_type == "sell":
            o_t = mt5.ORDER_TYPE_SELL
            s_l = price_a + stop_loss * point
            t_p = price_b - take_profit * point
            price=price_b
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