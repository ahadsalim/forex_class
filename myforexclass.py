import csv
import MetaTrader5 as mt5
import time
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
from itertools import product
import cufflinks as cf
from plotly.offline import iplot

class forex_backtest_class():
    '''
        tickers = List of tickers or symbol based on source data , if using yahoo use yahoo symbol and if using your data use your symbol as list
        start : start time as string
        end   : end time as string
        interval: period of data as string
        spread : spread of this instrument as string
        amount : How much capital do you want to trade with? as string
        source = source of data : none for using yahoo finance and "address of folder" to use your data ex :"c:/data" name of file must be "ticker.csv" and name of column of date must be "Datetime"
    '''

    def __repr__(self): 
        '''
        Present the Class
        '''
        return "Forex (start={} , end={} , interval={} )".format(self.start,self.end, self.interval)
    
    def __init__ (self ,tickers , start ,end ,interval , spread , amount , source=""):
        self.source= source
        self.tickers = tickers
        self.start= start
        self.end = end
        self.interval= interval
        self.spread = spread
        self.initial_amount = amount
        self.current_balance= amount
        self.units = 0
        self.trades= 0
        self.symbol=""
        self.data=pd.DataFrame()
        self.temp_data=pd.DataFrame()
        self.get_data()
        
    def get_data(self):
        '''
        Get Data from Yahoo Finance OR your csv file and calculate hold strategy
        '''
        data=pd.DataFrame()
        if self.source == ""  :
            for ticker in self.tickers :
                raw = yf.download (ticker , self.start , self.end , interval=self.interval)
                data[ticker+"_open"]=raw["Open"]
                data[ticker+"_close"]=raw["Close"]
                data[ticker+"_high"]=raw["High"]
                data[ticker+"_low"]=raw["Low"]
                data[ticker+"_volume"]=raw["Volume"]
                data[ticker+"_returns"] = np.log(data[ticker+"_close"] / data[ticker+"_close"].shift(1))
                data[ticker+"_cum_return"] = np.exp(data[ticker+"_returns"].cumsum())
                print("Data of {} downloded.".format(ticker))
        else :
            for ticker in self.tickers :
                address= self.source+"/"+ticker+".csv"
                raw=pd.read_csv(address, parse_dates=["Datetime"] , index_col=["Datetime"])
                raw=raw.loc[self.start:self.end].copy()
                raw=raw.resample(self.interval).last()
                data[ticker+"_open"]=raw["Open"]
                data[ticker+"_close"]=raw["Close"]
                data[ticker+"_high"]=raw["High"]
                data[ticker+"_low"]=raw["Low"]
                data[ticker+"_volume"]=raw["Volume"]
                data[ticker+"_returns"] = np.log(data[ticker+"_close"] / data[ticker+"_close"].shift(1))
                data[ticker+"_cum_return"] = np.exp(data[ticker+"_returns"].cumsum())
                print("Data of {} loded.".format(ticker))
                
        data.dropna(inplace=True)
        self.data=data.copy()
    
    def plot_data (self , columns=None) :
        '''
        columns = ['col1','col2',...]
        '''
        if columns is None :
            for ticker in self.tickers :
                columns=[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]
                self.data[columns].plot (figsize=(15,12) , title=ticker , secondary_y=ticker+"_close")
        else:
            self.data[columns].plot (figsize=(15,12) ,title="Your Plot")

    def get_perf_hold (self ,bar) :
        perf = self.temp_data.cum_return.iloc[bar]
        perf = round((perf -1 ) * 100 ,2)
        return perf
        
    def get_values(self ,bar):
        date= str(self.temp_data.index[bar].date())
        price= round(self.temp_data.Close.iloc[bar], 5)
        return date , price

    def buy_instrument(self , bar , units=None , amount=None):
        '''
        bar : In which Row (index) do trade
        units : How many units should buy
        amount : How much capital do you invest in this transaction?
        !!! One of units or amount must have value !!!
        '''
        date , price = self.get_values(bar)
        price1=price
        price += self.spread/2 
        if amount is not None :
            units= int(amount/price)
        self.current_balance -= units * price
        self.current_balance =round(self.current_balance,2)
        self.units += units
        self.trades +=1
        print ("{} Buying {} for {} with {} spread. Net price is {} and current balance is {}".format(date,units,round(price1,5),self.spread,round(price,5),self.current_balance))

    def go_long (self , bar , units=None , amount=None):
        '''
        bar : in which Row (index) do trade
        units : how many units should buy
        amount : How much capital do you invest in this transaction? ('all' or enter an integer)
        !!! One of units or amount must have value !!!
        '''
        if self.position == -1 :
            self.buy_instrument(bar , units= -self.units)
            print ("** Closed last short position !")
        if units:
            self.buy_instrument(bar, units= units)
        elif amount:
            if amount== "all" :
                amount = self.current_balance
            self.buy_instrument(bar , amount= amount)

    def sell_instrument(self , bar , units=None , amount=None):
        '''
        bar : In which Row (index) do trade
        units : How many units should sell
        amount : How much capital do you invest in this transaction?
        !!! One of units or amount must have value !!!
        '''
        date , price = self.get_values(bar)
        price1=price
        price -= self.spread/2
        if amount is not None :
            units= int(amount/price)
        self.current_balance += units * price
        self.current_balance =round(self.current_balance,2)
        self.units -= units
        self.trades +=1
        print ("{} Selling {} for {} with {} spread. Net price is {} and current balance is {}".format(date,units,round(price1,5),self.spread,round(price,5),self.current_balance))

    def go_short (self , bar , units=None , amount=None):
        '''
        bar : In which Row (index) do trade
        units : How many units should sell
        amount : How much capital do you invest in this transaction?
        !!! One of units or amount must have value !!!
        '''
        if self.position == 1 :
            self.sell_instrument(bar , units= self.units)
            print ("** Closed last long position !")
        if units:
            self.sell_instrument(bar , units= units)
        elif amount:
            if amount== "all" :
                amount = self.current_balance
            self.sell_instrument(bar , amount= amount)

    def close_position(self,ticker , bar) :
        '''
        bar : in which bar close all the positions
        '''
        date , price = self.get_values(bar)
        self.current_balance += self.units * price
        self.current_balance -= abs(self.units) * self.spread/2
        print(75 * "-")
        print("*** Summary of trading : {} ***".format(ticker))
        print("{} | Closing position of {} for {}".format(date,self.units,price))
        self.units=0
        self.trades +=1
        perf = ((self.current_balance- self.initial_amount) /self.initial_amount) * 100
        print("{} | Performance (%) = {}".format(date,round(perf,2)))
        print("{} | Number of Trades = {}".format(date,self.trades))
        print("{} | Performance of Buy and Hold Stategy (%)= {}".format(date, self.get_perf_hold(bar)))
        return round(perf,2), self.trades,self.get_perf_hold(bar) , self.print_current_Balance(bar)
        
    def print_current_position (self , bar) :
        '''
        bar : Print current position value in this bar
        '''
        date , price = self.get_values(bar)
        cpv = self.units * price
        print("{} | Current position value ={}".format(date,round(cpv,2)))

    def print_current_Balance (self , bar):
        '''
        bar : Print current balance value in this bar
        '''
        date , price = self.get_values(bar)
        print ("{} | Current Balance : {}".format(date, round(self.current_balance , 2)))
        return round(self.current_balance , 2)

    def print_current_nav(self , bar):
        '''
        bar : Print current net asset in this bar
        '''
        date , price = self.get_values( bar)
        nav = self.current_balance + self.units *price
        print("{} | Net Asset Value of = {}".format(date , round(nav,2)))

    def CAGR(self, column_name, period=None , days=365):
        '''
        Calculates the Compound Annual Growth Rate (CAGR) for a DataFrame.
        محاسبه نرخ رشد مرکب سالانه بر اساس تعداد روزهای کاری سال
        Args:
        column_name (str): The name of the column with Close or Adj_close.
        period (int, optional): The number of years in the period. 
        days (int, optional) : The number of days in a year. for FOREX= 252
        
        Returns:
            float: The CAGR value.
        '''
        df= self.data.copy()
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

        if len(df) < 2:
            raise ValueError("The DataFrame must contain at least two data points.")

        # Handle timestamps and period calculation
        if pd.api.types.is_datetime64_dtype(df.index):
            if not period:
                # Calculate period based on time difference between first and last timestamps
                period = (df.index[-1] - df.index[0]) / pd.Timedelta(days)
            else:
                # Ensure timestamps cover the specified period
                if (df.index[-1] - df.index[0]) < pd.Timedelta(years=period):
                    raise ValueError(f"Insufficient data for a {period}-year period. Data covers less time.")
        else:
            # Assume non-datetime index, use period directly if provided
            if not period:
                raise ValueError("Period required for non-datetime index.")

        # Calculate CAGR
        start_value = df[column_name].iloc[0]
        end_value = df[column_name].iloc[-1]
        cagr = ((end_value / start_value) ** (1 / period)) - 1

        return cagr

    def volatility(self , column_name, period=365):
        '''
        Calculates the annualized volatility.
        نوسان پذیری به میزان عدم قطعیت یا ریسک میزان تغییرات ارزش هر نوع دارایی
        هر چه بیشتر باشد سرمایه گذاری پرمخاطره تر است
        Args:
            column_name (str): The name of the column with Close or Adj_close.
            period (int, optional): The number of trading days in the period (for FOREX= 252).

        Returns:
            float: The annualized volatility value.

        Raises:
            ValueError: If the column is not found or there are less than two data points.
        '''
        df= self.data.copy()
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

        if len(df) < 2:
            raise ValueError("The DataFrame must contain at least two data points.")

        # Calculate daily logarithmic returns
        returns = np.log(df[column_name].pct_change() + 1)

        # Calculate standard deviation of returns
        std_dev = returns.std()

        # Annualize volatility by multiplying by square root of period
        annualized_volatility = std_dev * np.sqrt(period)

        return annualized_volatility


    def sharpe_ratio(self, column_name, risk_free_rate=0.0):
        '''
        Calculates the Sharpe Ratio .
        محاسبه نسبت شارپ : میانگین بازده بدست آمده مازاد بر نرخ سود بدون رسیک به ازای هر واحد
        این نسبت بیشتر برای مقایسه تغییرات ریسک وقتی دارایی جدیدی اضافه می شود به کار می آید.
        عدد بیشتر از یک مورد قبول و بیشتر از 2 خیلی خوب و بیشتر از 3 عالی است اما کمتر از یک بد است.
        Args:
            column_name (str): The name of the column with Close or Adj_close.
            risk_free_rate (float, optional): The annualized risk-free rate. Defaults to 0.0.

        Returns:
            float: The Sharpe Ratio value.
        '''
        df= self.data.copy()
        sharpe_ratio = (self.CAGR(column_name) - risk_free_rate)/self.volatility(column_name)

        return sharpe_ratio

    def sortino_ratio(self, column_name, risk_free_rate=0.0):
        '''
            Calculates the Sortino Ratio for a DataFrame.
            این نسبت مانند نسبت شارپ است با این تفاوت که در این نسبت فقط ریسک منفی در نظر گرفته می شود
              و مانند نسبت شارپ هر چه از 1 بیشتر باشد بهتر است.
        Args:
            column_name (str): The name of the column with Close or Adj_close.
            risk_free_rate (float, optional): The annualized risk-free rate. Defaults to 0.0.

        Returns:
            float: The Sortino Ratio value.

        Raises:
            ValueError: If the column is not found or there are less than two data points.
        '''
        df= self.data.copy()
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

        if len(df) < 2:
            raise ValueError("The DataFrame must contain at least two data points.")

        # Calculate daily logarithmic returns
        returns = np.log(df[column_name].pct_change() + 1)

        # Calculate downside returns (negative returns only)
        downside_returns = returns[returns < 0]

        # Calculate squared downside returns
        squared_downside_returns = downside_returns**2

        # Calculate standard deviation of downside returns (annualized)
        std_downside_return = np.sqrt(squared_downside_returns.mean()) * np.sqrt(252)  # Assuming daily data

        # Calculate average excess return (same as Sharpe Ratio calculation)
        avg_excess_return = returns.mean() - risk_free_rate

        # Calculate Sortino Ratio
        if std_downside_return == 0:
            # Avoid division by zero (handle case of no downside risk)
            sortino_ratio = np.inf
        else:
            sortino_ratio = avg_excess_return / std_downside_return

        return sortino_ratio

    def max_drawdown(self, column_name):
        '''
            Calculates the Maximum Drawdown (MDD) for a DataFrame.

            Args:
                column_name (str): The name of the column with Close or Adj_close.  

            Returns:
                tuple: A tuple containing:
                    - float: The maximum drawdown percentage (negative value).
                    - int: The index of the row where the maximum drawdown starts.

            Raises:
                ValueError: If the column is not found or there are less than two data points.
        '''
        df= self.data.copy()
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

        if len(df) < 2:
            raise ValueError("The DataFrame must contain at least two data points.")

        # Calculate cumulative returns (avoiding division by zero)
        cumulative_returns = (df[column_name].pct_change() + 1).cumprod()
        # Calculate running maximum
        rolling_max = cumulative_returns.rolling(window=len(cumulative_returns)).max()
        # Calculate drawdown
        drawdown = cumulative_returns - rolling_max
        # Find the maximum drawdown
        max_drawdown = drawdown.min()
        max_drawdown_idx = drawdown.idxmin()

        return max_drawdown, max_drawdown_idx

    def calmar_ratio(self , column_name):
        "function to calculate calmar ratio"
        clmr = self.CAGR(column_name)/self.max_drawdown(column_name)
        return clmr

    # ***************************************************** Simple Moving Average ***********************************
    def sma(self , ticker ,short , long) :
        '''
        Calculate Simple Moving Average Strategy
        '''
        df=self.data[[ticker+"_close",ticker+"_returns"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns"},inplace=True)
        
        df["sma_s"]=df.Close.rolling(short).mean() #calculate average of short period
        df["sma_l"]=df.Close.rolling(long).mean()
        df.dropna(inplace=True)
        df["pos"]= np.where(df.sma_s>df.sma_l,1,-1) # position of buy (1) or sell (-1)
        df["trades"]= df.pos.diff().fillna(0).abs()
        df["str_sma"]= df.pos.shift(1)* df.returns
        df["str_net"]= df.str_sma - (df.trades * (self.spread/2))
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        df.dropna(inplace=True)
        perf = round(df["cum_str_net"].iloc[-1] , 5)
        return perf
    
    def best_sma(self , ticker):
        '''
        It examines the SMA strategy and declares the best short and long time periods with a higher profit target.
        '''
        maxlen=len(self.data[ticker+"_close"])
        if maxlen <= 50 :
            sma_s = range(5,10,1)
            sma_l = range(10,maxlen,1)
        elif maxlen >50 and maxlen <200 :
            sma_s = range(9,50,1)
            sma_l = range(51,maxlen,1)
        else :
            sma_s = range(9,50,1)
            sma_l = range(51,200,1)
        couple=list(product(sma_s,sma_l))
        results=[]
        for c in couple :
            results.append(self.sma(ticker , c[0] , c[1]))
        return couple[np.argmax(results)]
    
    def sma_backtest(self, ticker ,SMA_S ,SMA_L ):
        '''
        Back testing for SMA 
        SMA_S : Short period simple moving average 
        SMA_L : Long period simple moving average 
        '''
        print("Testing SMA Strategy | {} | SMA_S= {} | SMA_L= {}".format(ticker , SMA_S,SMA_L))
        print(75 * "-")

        self.position=0
        self.trades=0
        print ("Initial amount is : {}".format(self.initial_amount))
        self.current_balance = self.initial_amount
        
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["SMA_S"] = df["Close"].rolling(SMA_S).mean()
        df["SMA_L"] = df["Close"].rolling(SMA_L).mean()
        df.dropna(inplace =True)
        self.temp_data=df.copy()

        for bar in range(len(df)-1) :
            if df["SMA_S"].iloc[bar] > df["SMA_L"].iloc[bar] :
                if self.position in [0,-1] :
                    self.go_long(bar , amount="all")
                    self.position =1
            if df["SMA_S"].iloc[bar] < df["SMA_L"].iloc[bar] :
                if self.position in [0,1] :
                    self.go_short(bar , amount="all")
                    self.position = -1
        summary= self.close_position(ticker ,bar+1)
        return summary

    # ************************************************* Exponential Moving Average ******************************************
    def ema(self , ticker , short , long) :
        '''
        Calculate Exponential Moving Average Strategy
        '''
        df=self.data[[ticker+"_close",ticker+"_returns"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns"},inplace=True)

        df["ema_s"]=df.Close.ewm(span=short , min_periods= short).mean() #calculate average of short period
        df["ema_l"]=df.Close.ewm(span=long , min_periods= long).mean()
        df.dropna(inplace=True)
        df["pos"]= np.where(df.ema_s>df.ema_l,1,-1) # position of buy (1) or sell (-1)
        df["trades"]= df.pos.diff().fillna(0).abs()
        df["str_sma"]= df.pos.shift(1)* df.returns
        df["str_net"]= df.str_sma - (df.trades * (self.spread/2))
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        df.dropna(inplace=True)
        perf = round(df["cum_str_net"].iloc[-1] , 5)
        return perf

    def best_ema(self , ticker):
        '''
        It examines the EMA strategy and declares the best short and long time periods with a higher profit target.
        '''
        maxlen=len(self.data)
        if maxlen <= 50 :
            ema_s = range(5,10,1)
            ema_l = range(10,maxlen,1)
        elif maxlen >50 and maxlen <200 :
            ema_s = range(9,50,1)
            ema_l = range(51,maxlen,1)
        else :
            ema_s = range(9,50,1)
            ema_l = range(51,200,1)
        couple=list(product(ema_s,ema_l))
        results=[]
        for c in couple :
            results.append(self.ema(ticker , c[0] , c[1]))
        return couple[np.argmax(results)]

    def ema_backtest(self ,ticker , EMA_S ,EMA_L ):
        '''
        Back testing for EMA 
        EMA_S : Short period exponential moving average 
        EMA_L : Long period exponential moving average 
        '''
        print("Testing EMA Strategy | {} | EMA_S= {} | EMA_L= {}".format(ticker ,EMA_S,EMA_L))
        print(75 * "-")

        self.position=0
        self.trades=0
        print ("Initial amount is : {}".format(self.initial_amount))
        self.current_balance = self.initial_amount
        
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["EMA_S"] = df["Close"].ewm(span=EMA_S , min_periods= EMA_S).mean()
        df["EMA_L"] = df["Close"].ewm(span=EMA_L , min_periods= EMA_L).mean()
        df.dropna(inplace =True)
        self.temp_data=df.copy()

        for bar in range(len(df)-1) :
            if df["EMA_S"].iloc[bar] > df["EMA_L"].iloc[bar] :
                if self.position in [0,-1] :
                    self.go_long(bar , amount="all")
                    self.position =1
            if df["EMA_S"].iloc[bar] < df["EMA_L"].iloc[bar] :
                if self.position in [0,1] :
                    self.go_short(bar , amount="all")
                    self.position = -1
        summary= self.close_position(ticker ,bar+1)
        return summary

    #*************************************************** Double Exponential Moving Average strategy ******************
    def dema( self , ticker ,short , long ):
        df=self.data[[ticker+"_close",ticker+"_returns"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns"},inplace=True)

        df["returns"]= np.log(df.Close.div(df.Close.shift(1)))
        df.dropna(inplace=True)
        EMA = df["Close"].ewm(span=short , adjust = False).mean()
        df["DEMA_S"] = 2*EMA - EMA.ewm(span=short , adjust = False).mean()
        EMA = df["Close"].ewm(span=long , adjust = False).mean()
        df["DEMA_L"] = 2*EMA - EMA.ewm(span=short , adjust = False).mean()
        df["pos"] = np.where(df['DEMA_S'] > df['DEMA_L'] , 1 , -1 )
        df["trades"]= df.pos.diff().fillna(0).abs()
        df["str_dema"]= df.pos.shift(1)* df.returns
        df["str_net"]= df.str_dema - (df.trades * (self.spread/2))
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        df.dropna(inplace=True)
        perf = round(df["cum_str_net"].iloc[-1] , 5)
        return perf
    
    def best_dema(self , ticker):
        '''
        It examines the DEMA strategy and declares the best short and long time periods with a higher profit target.
        '''
        maxlen=len(self.data)
        if maxlen <= 80 :
            dema_s = range(10,20,1)
            dema_l = range(20,maxlen,1)
        else :
            dema_s = range(10,40,1)
            dema_l = range(41,80,1)
        couple=list(product(dema_s,dema_l))
        results=[]
        for c in couple :
            results.append(self.dema(ticker ,c[0],c[1]))
        return couple[np.argmax(results)]

    def dema_backtest(self ,ticker , short ,long ):
        '''
        Back testing for DEMA 
        short : Short period exponential moving average 
        long  : Long period exponential moving average 
        '''
        print("Testing DEMA Strategy | {} | Short= {} | Long= {}".format(ticker ,short,long))
        print(75 * "-")

        self.position=0
        self.trades=0
        print ("Initial amount is : {}".format(self.initial_amount))
        self.current_balance = self.initial_amount
        
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["returns"]= np.log(df.Close.div(df.Close.shift(1)))
        df.dropna(inplace=True)
        EMA = df["Close"].ewm(span=short , adjust = False).mean()
        df["DEMA_S"] = 2*EMA - EMA.ewm(span=short , adjust = False).mean()
        EMA = df["Close"].ewm(span=long , adjust = False).mean()
        df["DEMA_L"] = 2*EMA - EMA.ewm(span=short , adjust = False).mean()
        df["pos"] = np.where(df['DEMA_S'] > df['DEMA_L'] , 1 , -1 )
        df["trades"]= df.pos.diff().fillna(0).abs()
        df["str_dema"]= df.pos.shift(1)* df.returns
        df["str_net"]= df.str_dema - (df.trades * (self.spread/2))
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        df.dropna(inplace=True)
        self.temp_data=df.copy()

        for bar in range(len(df)-1) :
            if df["DEMA_S"].iloc[bar] > df["DEMA_L"].iloc[bar] :
                if self.position in [0,-1] :
                    self.go_long(bar , amount="all")
                    self.position =1
            if df["DEMA_S"].iloc[bar] < df["DEMA_L"].iloc[bar] :
                if self.position in [0,1] :
                    self.go_short(bar , amount="all")
                    self.position = -1
        summary= self.close_position(ticker ,bar+1)
        return summary
    # ****************************************** Relative Strength Index Indicator ******************************
    def rsi(self , ticker ,period=14 ,ma_down=30 , ma_up=70 ):
        '''
        Calculate Relative Strength Index
        '''
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["returns"]= np.log(df.Close.div(df.Close.shift(1)))
        df.dropna(inplace=True)
        df["UP"]= np.where( df.Close.diff() > 0 , df.Close.diff() , 0) # Height of Green candle (diffrence between last price and before)
        df["DOWN"] = np.where( df.Close.diff() < 0 , -df.Close.diff() , 0) # Height of Red candle
        df["MA_UP"]= df.UP.rolling(int(period)).mean()
        df["MA_DOWN"]= df.DOWN.rolling(int(period)).mean()
        df["RSI"]= df.MA_UP / (df.MA_UP + df.MA_DOWN) *100
        df.dropna(inplace=True)
        rsi_up= int(ma_up)
        rsi_down= int(ma_down)
        df["pos"]= np.where( df.RSI > rsi_up , -1 , np.nan) # Sell warning
        df["pos"]= np.where( df.RSI < rsi_down , 1 , df.pos) # Buy warning
        df.pos = df.pos.fillna(0)
        df.dropna(inplace=True)
        df["trades"]= df.pos.diff().fillna(0).abs()
        df["str_rsi"]= df.pos.shift(1)* df.returns
        df["str_net"]= df.str_rsi - (df.trades * (self.spread/2))
        df.dropna(inplace=True)
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        perf = round(df["cum_str_net"].iloc[-1] , 5)
        return perf

    def best_rsi(self , ticker):
        '''
        It examines the RSI strategy and declares the best period and up and down moving average with a higher profit target.
        '''
        maxlen=len(self.data)
        if maxlen <= 85 :
            ma_down = range(20,40,1)
            ma_up   = range(60,maxlen,1)
        else :
            ma_down = range(20,40,1)
            ma_up   = range(60,85,1)
        period = range(5,20,1)
        couple=list(product(period,ma_down,ma_up))
        results=[]
        for c in couple :
            results.append(self.rsi(ticker ,c[0],c[1],c[2]))
        if (len(results)) :
            return couple[np.argmax(results)]
        else : 
            return "There is no position to trade !"
        
    def rsi_backtest(self , ticker ,period ,ma_down , ma_up ):
        '''
        Back testing for RSI 
        period : 
        Ema_down :  
        ma_up:
        '''
        print("Testing RSI Strategy | {} | Period= {} | MA_Down= {} | MA_Up= {}".format(self.symbol ,period,ma_down,ma_up))
        print(75 * "-")

        self.position=0
        self.trades=0
        print ("Initial amount is : {}".format(self.initial_amount))
        self.current_balance = self.initial_amount
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["returns"]= np.log(df.Close.div(df.Close.shift(1)))
        df.dropna(inplace=True)
        df["UP"]= np.where( df.Close.diff() > 0 , df.Close.diff() , 0) # Height of Green candle (diffrence between last price and before)
        df["DOWN"] = np.where( df.Close.diff() < 0 , - df.Close.diff() , 0) # Height of Red candle
        df["MA_UP"]= df.UP.rolling(int(period)).mean()
        df["MA_DOWN"]= df.DOWN.rolling(int(period)).mean()
        df["RSI"]= df.MA_UP / (df.MA_UP + df.MA_DOWN) *100
        df.dropna(inplace=True)
        rsi_up= int(ma_up)
        rsi_down= int(ma_down)
        self.temp_data=df.copy()

        for bar in range(len(df)-1) :
            if df["RSI"].iloc[bar] < rsi_down :
                if self.position in [0,-1] :
                    self.go_long(bar , amount="all")
                    self.position =1
            if df["RSI"].iloc[bar] > rsi_up :
                if self.position in [0,1] :
                    self.go_short(bar , amount="all")
                    self.position = -1
        summary= self.close_position(ticker ,bar+1)
        return summary

    #*************************************************************************
    def macd (self , ticker ,EMA_S , EMA_L , Signal):
        '''
        Calculate Moving average convergence/divergence Strategy
        '''
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["EMA_S"] = df["Close"].ewm(span=EMA_S , min_periods= EMA_S).mean()
        df["EMA_L"] = df["Close"].ewm(span=EMA_L , min_periods= EMA_L).mean()
        df["MACD"]= df["EMA_S"] - df["EMA_L"]
        df["MACD_Signal"] = df.MACD.ewm(span=Signal , min_periods=Signal).mean()
        df.dropna( inplace=True)
        df["pos"]= np.where(df.MACD - df.MACD_Signal > 0 , 1, -1) # position of buy (1) or sell (-1)
        df["trades"]= df.pos.diff().fillna(0).abs()
        df["str_macd"]= df.pos.shift(1)* df.returns
        df["str_net"]= df.str_macd - (df.trades * (self.spread/2))
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        perf = round(df["cum_str_net"].iloc[-1] , 5)
        return perf

    def best_macd(self , ticker):
        '''
        It examines the MACD strategy and declares the best short and long and signal time periods with a higher profit target.
        '''
        maxlen=len(self.data)
        if maxlen <= 40 :
            ema_s = range(5,20,1)
            ema_l = range(20,maxlen,1)
        else :
            ema_s = range(5,20,1)
            ema_l = range(20,40,1)
        signal = range(5,15,1)
        couple=list(product(ema_s,ema_l,signal))
        results=[]
        for c in couple :
            results.append(self.macd(ticker ,c[0],c[1],c[2]))
        if (len(results)) :
            return couple[np.argmax(results)]
        else : 
            return "There is no position to trade !"

    def macd_backtest(self ,ticker , EMA_S ,EMA_L , Signal):
        '''
        Back testing for MACD 
        EMA_S : Short period exponential moving average 
        EMA_L : Long period exponential moving average 
        Signal: The period of (EMA_S - EMA_L) moving average
        '''
        print("Testing MACD Strategy | {} | EMA_S= {} | EMA_L= {} | Signal= {}".format(self.symbol ,EMA_S,EMA_L,Signal))
        print(75 * "-")

        self.position=0
        self.trades=0
        print ("Initial amount is : {}".format(self.initial_amount))
        self.current_balance = self.initial_amount
        
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["EMA_S"] = df["Close"].ewm(span=EMA_S , min_periods= EMA_S).mean()
        df["EMA_L"] = df["Close"].ewm(span=EMA_L , min_periods= EMA_L).mean()
        df["MACD"]= df["EMA_S"] - df["EMA_L"]
        df["MACD_Signal"] = df.MACD.ewm(span=Signal , min_periods=Signal).mean()
        df.dropna( inplace=True)
        self.temp_data=df.copy()

        for bar in range(len(df)-1) :
            if df["MACD"].iloc[bar] - df["MACD_Signal"].iloc[bar] > 0 :
                if self.position in [0,-1] :
                    self.go_long(bar , amount="all")
                    self.position =1
            if df["MACD"].iloc[bar] - df["MACD_Signal"].iloc[bar] < 0 :
                if self.position in [0,1] :
                    self.go_short(bar , amount="all")
                    self.position = -1
        summary= self.close_position(ticker ,bar+1)
        return summary

    #************************************************************************* Bollinger Band Indicator **************************
    def bollinger (self , ticker ,sma , dev ) :
        '''
        Calculate Bollinger Band
        '''
        self.position=0
        self.trades=0
        
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["sma"]=df.Close.rolling(sma).mean()
        df["lower"]= df["sma"] - dev * df["Close"].rolling(sma).std()
        df["upper"]= df["sma"] + dev * df["Close"].rolling(sma).std()
        df.dropna(inplace=True)
        df["position"]=np.where (df.Close < df.lower ,1 , np.nan)
        df["position"]=np.where (df.Close > df.upper ,-1 , df["position"])
        df["distance"]= df.Close - df.sma
        df["position"]= np.where( df.distance * df.distance.shift(1) <0 , 0, df["position"])
        df["position"]= df.position.ffill().fillna(0)
        df["trades"]= df.position.diff().fillna(0).abs()
        df["str_boll"]= df.position.shift(1)* df.returns
        df["str_net"]= df.str_boll - (df.trades * (self.spread/2))
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        df.dropna(inplace=True)
        perf = round(df["cum_str_net"].iloc[-1] , 5)
        return perf
    
    def best_bollinger(self ,ticker):
        '''
        It examines the Bollinger Band strategy and declares the best SMA and Deviation with a higher profit target.
        '''
        maxlen=len(self.data)
        if maxlen <= 50 :
            sma = range(10,maxlen,1)
        else :
            sma = range(10,50,1)
        dev = range(1,5,1)
        couple=list(product(sma,dev))
        results=[]
        for c in couple :
            results.append(self.bollinger(ticker ,c[0],c[1]))
        return couple[np.argmax(results)]
        
    def bollinger_backtest (self, ticker ,SMA , dev): # ************** شروط معامله دوباره کنترل شود. مشکل دارد خرید با مقدار منفی انجام می دهد
        '''
        Back Testing for Bollinger bands strategy
        '''
        print("Testing Bollinger Band Strategy | {} | SMA= {} | dev= {}".format(self.symbol , SMA,dev))
        print(75 * "-")

        self.position=0
        self.trades=0
        self.current_balance=self.initial_amount
        
        df=self.data[[ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["SMA"] = df.Close.rolling(SMA).mean()
        df["Lower"] = df["SMA"]- df.Close.rolling(SMA).std() * dev
        df["Upper"] = df["SMA"]+ df.Close.rolling(SMA).std() * dev
        df.dropna(inplace = True)
        self.temp_data=df.copy()

        for bar in range(len(df)-1) :
            if self.position ==0 :
                if df["Close"].iloc[bar] < df["Lower"].iloc[bar] :
                    self.go_long (bar , amount="all")
                    self.position = 1
                elif df["Close"].iloc[bar] > df["Upper"].iloc[bar] :
                    self.go_short (bar , amount="all")
                    self.position = -1
            
            elif self.position ==1 :
                if df["Close"].iloc[bar] > df["SMA"].iloc[bar] :
                    if df["Close"].iloc[bar] > df["Upper"].iloc[bar]:
                        self.go_short (bar , amount="all")
                        self.position = -1
                    else :
                        self.go_short(bar , units= self.units)
                        self.position=0
            
            elif self.position == -1 :            
                if df["Close"].iloc[bar] < df["SMA"].iloc[bar] :
                    if df["Close"].iloc[bar] < df["Lower"].iloc[bar]:
                        self.go_long (bar , amount="all")
                        self.position = 1
                    else :
                        self.go_long(bar , units= -self.units)
                        self.position=0
        
        summary= self.close_position(ticker ,bar+1)
        return summary

    #*********************************************************** Stochastic Oscilator ******************************************        
    def stochastic(self ,ticker , K ,D ) :
        '''
        Calculate Stochastic Oscilator
        '''
        
        df=self.data[[ticker+"_low",ticker+"_high",ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_low":"Low",ticker+"_high":"High",ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["roll_low"]= df.Low.rolling(int(K)).min()
        df["roll_high"] = df.High.rolling(int(K)).max() 
        df["K"]= (df.Close - df.roll_low) / (df.roll_high - df.roll_low) * 100
        df["D"]= df.K.rolling(int(D)).mean()
        df["pos"]= np.where( df["K"] > df["D"] , 1 , -1) 
        df.dropna(inplace=True)
        df["trades"]= df.pos.diff().fillna(0).abs()
        df["str_stochastic"]= df.pos.shift(1)* df.returns
        df["str_net"]= df.str_stochastic - (df.trades * (self.spread/2))
        df.dropna(inplace=True)
        df["cum_str_net"] = df.str_net.cumsum().apply(np.exp)
        perf = round(df["cum_str_net"].iloc[-1] , 5)
        return perf
    
    def best_stochastic(self ,ticker):
        '''
        It examines the Stochastic strategy and declares the best K and D with a higher profit target.
        '''
        k = range(11,30,1)
        d = range(2,10,1)

        couple=list(product(k,d))
        results=[]
        for c in couple :
            results.append(self.stochastic(ticker,c[0],c[1]))
        if (len(results)) :
            return couple[np.argmax(results)]
        else : 
            return "There is no position to trade !"
        
    def stochastic_backtest(self ,ticker , K, D ):
        '''
        Back testing for Stochastic
        K : 
        D :  
        '''
        print("Testing Stochastic Strategy | {} | K= {} | D= {} ".format(self.symbol ,K,D))
        print(75 * "-")

        self.position=0
        self.trades=0
        print ("Initial amount is : {}".format(self.initial_amount))
        self.current_balance = self.initial_amount
        
        df=self.data[[ticker+"_low",ticker+"_high",ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_low":"Low",ticker+"_high":"High",ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["roll_low"]= df.Low.rolling(int(K)).min()
        df["roll_high"] = df.High.rolling(int(K)).max() 
        df["K"]= (df.Close - df.roll_low) / (df.roll_high - df.roll_low) * 100
        df["D"]= df.K.rolling(int(D)).mean()
        df.dropna(inplace=True)
        self.temp_data=df.copy()

        for bar in range(len(df)-1) :
            if df["K"].iloc[bar] > df["D"].iloc[bar] :
                if self.position in [0,-1] :
                    self.go_long(bar , amount="all")
                    self.position =1
            if df["K"].iloc[bar] < df["D"].iloc[bar] :
                if self.position in [0,1] :
                    self.go_short(bar , amount="all")
                    self.position = -1
        summary= self.close_position(ticker ,bar+1)
        return summary
    
    #************************************************************ Average True Range Indicator ************************
    def atr(self , ticker ,n ):
        '''
        Calculate ATR Indicator 
        n=14 , most of the time
        '''
        df=self.data[[ticker+"_low",ticker+"_high",ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_low":"Low",ticker+"_high":"High",ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df["H-L"]= abs(df["High"] - df["Low"])
        df["H-PC"] = abs(df["High"] - df["Close"].shift(1)) 
        df["L-PC"]= abs(df["Low"] - df["Close"].shift(1))
        df["TR"]= df[["H-L" , "H-PC" , "L-PC"]].max(axis=1 , skipna=False)
        df["ATR"]= df["TR"].rolling(n).mean()
        df.dropna(inplace=True)
        #******************************** incomplete ************************

        return df["TR"]
    
    #********************************************** Average Directional Movement Index (ADX) indicator ******************** Error !!!!!!!!!!!!!!!!
    def adx(self , ticker , period=14 ):
        '''
        Calculates the Average Directional Movement Index (ADX) indicator
        period (int, optional): The lookback period for the calculations. Defaults to 14.
        این اندیکاتور برای تعیین وضعیت حالت دارای روند و بدون روند استفاده می شود.
        Returns:
        pandas.DataFrame: The DataFrame with additional columns named:
            - 'plus_di': Positive Directional Indicator (DI+) values.
            - 'minus_di': Negative Directional Indicator (DI-) values.
            - 'adx': Average Directional Movement Index (ADX) values.
        '''
        df=self.data[[ticker+"_low",ticker+"_high",ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_low":"Low",ticker+"_high":"High",ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        # Calculate True Range (TR)
        df['tr'] = df['High'] - df['Low']
        df['tr'] = df[['tr', abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift(1))]].max(axis=1)

        # Calculate Average True Range (ATR)
        df['atr'] = df['tr'].rolling(window=period).mean()

        # Calculate Positive Directional Indicator (DI+)
        df['upmove'] = df['High'] - df['High'].shift(1)
        df['upmove'] = df[['upmove', 0]].max(axis=1)
        df['upmove'] = np.where(df['upmove'] > df['Low'] - df['Close'].shift(1), df['upmove'], 0)
        df['plus_di'] = df['upmove'].rolling(window=period).mean() / df['atr']

        # Calculate Negative Directional Indicator (DI-)
        df['downmove'] = df['Low'].shift(1) - df['low']
        df['downmove'] = df[['downmove', 0]].max(axis=1)
        df['downmove'] = np.where(df['downmove'] > df['High'].shift(1) - df['Close'], df['downmove'], 0)
        df['minus_di'] = df['downmove'].rolling(window=period).mean() / df['atr']

        # Calculate Smoothed True Range (SMI)
        df['smi'] = df['plus_di'] - df['minus_di']
        df['smi'] = abs(df['smi']) * 100 / df['atr'].rolling(window=period - 1).mean()

        # Calculate ADX
        df['adx'] = df['smi'].rolling(window=period - 1).mean()

        # Drop unnecessary columns
        df.drop(columns=['tr', 'atr', 'upmove', 'downmove', 'smi'], inplace=True)
        
        self.temp_data=df.copy()
    #********************************************************** On Balance Volume Indicator **************************
    def OBV(self ,ticker):
        '''
        Calculate On Balance Volume Indicator
        '''
        df=self.data[[ticker+"_volume",ticker+"_close",ticker+"_returns",ticker+"_cum_return"]].copy()
        df.rename(columns={ticker+"_volume":"Volume",ticker+"_close":"Close",ticker+"_returns":"returns",ticker+"_cum_return":"cum_return"},inplace=True)

        df['obv'] = 0
        for i in range(1, len(df)):
            if df.loc[i, 'Close'] > df.loc[i-1, 'Close']:
                df.loc[i, 'obv'] = df.loc[i-1, 'obv'] + df.loc[i, 'Volume']
            elif df.loc[i, 'Close'] < df.loc[i-1, 'Close']:
                df.loc[i, 'obv'] = df.loc[i-1, 'obv'] - df.loc[i, 'Volume']
            else:
                df.loc[i, 'obv'] = df.loc[i-1, 'obv']
        return df['obv']




    def test_strategies (self , ticker) :
        #intervals = ["1m","2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"]
        intervals = ["1h", "1d"]
        report=["Ticker","Interval","Strategy","Best_Param","Num_Trades","Perf_Hold","Perf_Strategy","Current_Balance"]
        filename= ticker+"_report.csv"
        with open(filename, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(report)
            for interv in intervals :
                self.interval = interv
                self.get_data(ticker)
                sma=self.best_sma()
                summary_sma=self.sma_strategy(sma[0],sma[1])
                csv_writer.writerow([ticker ,interv ,"SMA", str(sma) ,str(summary_sma[1]), str(summary_sma[2]) , str(summary_sma[0]) , str(summary_sma[3])])

                ema = self.best_ema()
                summary_ema= self.ema_strategy (ema[0],ema[1])
                csv_writer.writerow([ticker ,interv ,"EMA", str(ema) ,str(summary_ema[1]), str(summary_ema[2]) , str(summary_ema[0]) , str(summary_ema[3])])

                macd= self.best_macd()
                summary_macd = self.macd_strategy(macd[0],macd[1], macd[2])
                csv_writer.writerow([ticker ,interv ,"MACD", str(macd) ,str(summary_macd[1]), str(summary_macd[2]) , str(summary_macd[0]) , str(summary_macd[3])])

                rsi = self.best_rsi()
                summary_rsi = self.rsi_strategy(rsi[0],rsi[1],rsi[2])
                csv_writer.writerow([ticker ,interv ,"RSI", str(rsi) ,str(summary_rsi[1]), str(summary_rsi[2]) , str(summary_rsi[0]) , str(summary_rsi[3])])

                boll= self.best_bollinger()
                summary_boll= self.bollinger_strategy(boll[0],boll[1])
                csv_writer.writerow([ticker ,interv ,"Bollinger Band", str(boll) ,str(summary_boll[1]), str(summary_boll[2]) , str(summary_boll[0]) , str(summary_boll[3])])

                stochastic = self.best_stochastic()
                summary_stochastic = self.stochastic_strategy(stochastic[0],stochastic[1])
                csv_writer.writerow([ticker ,interv ,"Stochastic", str(stochastic) ,str(summary_stochastic[1]), str(summary_stochastic[2]) , str(summary_stochastic[0]) , str(summary_stochastic[3])])
                
                print("{} intervals was done".format(interv))
        print("The Operation is over !")       

