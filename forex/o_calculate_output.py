import myforexclass as mf
import datetime

start_year = 2024
start_month = 1
end_year = 2024
end_month = 2
interval = "1h"
tickers=["ETH-USD"]
result_data = mf.pd.DataFrame(columns=["date","ticker","return_hold",
                                       "short_sma","long_sma","trade_sma","return_sma",
                                       "short_ema","long_ema","trade_ema","return_ema",
                                       "short_dema","long_dema","trade_dema","return_dema",
                                       "rsi_period","rsi_m_down","rsi_m_up","trade_rsi","return_rsi",
                                       "macd_s","macd_l","macd_signal","trade_macd","return_macd",
                                       "sma_bollinger","dev_bollinger","trade_bollinger","return_bollinger",
                                       "stoc_k","stoc_d","trade_bollinger","retun_stochastic",
                                       "trade_Ichimoku , return_Ichimoku"])

# حلقه برای دریافت داده‌های هر ماه
year = start_year
month = start_month
for ticker in tickers :
    while year < end_year or (year == end_year and month <= end_month):
        start_month_date = datetime.date(year, month, 1)
        print(f"دریافت داده‌های ماه {month}-{year} نماد {ticker}")
        if month == 12:
            end_month_date = datetime.date(year+1, 1, 1) - datetime.timedelta(days=1)
            year += 1
            month = 1
        else:
            end_month_date = start_month_date.replace(month=month+1) - datetime.timedelta(days=1)
            month += 1

        t = mf.forex_backtest_class([ticker],start_month_date , end_month_date , interval ,0,1000)
        #******************************** SMA
        (s_sma,l_sma)= t.best_param_sma(ticker)
        out_sma=t.sma_backtest(ticker,s_sma,l_sma)
        #******************************* EMA
        (s_ema,l_ema)= t.best_param_ema(ticker)
        out_ema=t.ema_backtest(ticker,s_ema,l_ema)
        #******************************* DMA
        (s_dema,l_dema)= t.best_param_dema(ticker)
        out_dema=t.dema_backtest(ticker,s_dema,l_dema)
        #******************************* RSI
        (p_rsi,d_rsi,u_rsi)= t.best_param_rsi(ticker)
        out_rsi=t.rsi_backtest(ticker,p_rsi,d_rsi,u_rsi)
        #******************************* MACD
        (s_macd,l_macd,signal_macd)= t.best_param_macd(ticker)
        out_macd=t.macd_backtest(ticker,s_macd,l_macd,signal_macd)
        #******************************* Bollinger
        (sma_bol,dev_bol)= t.best_param_bollinger(ticker)
        out_bol=t.bollinger_backtest(ticker,sma_bol,dev_bol)
        #******************************* Stochastic
        (k_stoc,d_stoc)= t.best_param_stochastic(ticker)
        out_stoc=t.stochastic_backtest(ticker,k_stoc,d_stoc)
        #******************************* Ichimuko
        out_ichi= t.ichimoku_backtest(ticker)
        
        new_row = mf.pd.Series([end_month_date,ticker ,out_sma[2],
                                s_sma, l_sma, out_sma[1],out_sma[0],
                                s_ema,l_ema,out_ema[1],out_ema[0],
                                s_dema,l_dema,out_dema[1],out_dema[0],
                                p_rsi,d_rsi,u_rsi,out_rsi[1],out_rsi[0],
                                s_macd,l_macd,signal_macd,out_macd[1],out_macd[0],
                                sma_bol,dev_bol,out_bol[1],out_bol[0],
                                k_stoc,d_stoc,out_stoc[1],out_stoc[0],
                                out_ichi[1],out_ichi[0]]
                                , index=result_data.columns)
        result_data = mf.pd.concat([result_data, new_row.to_frame().T], ignore_index=True)

result_data.set_index('date', inplace=True)
result_data.to_csv("output.csv")
print(result_data)
