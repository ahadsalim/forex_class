import MetaTrader5 as mt5
import myforexclass as mf

class portfo_forex() :
    def __repr__(self): 
        '''
        Present the Class
        '''
        return "Forex (start={} , end={} , interval={} )".format(self.start,self.end, self.interval)
    
    def __init__ (self ,tickers, period):
        self.tickers = tickers
        self.period = period
    
    
    def mean_param_sma(self , ticker , period):
        
