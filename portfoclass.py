import MetaTrader5 as mt5
import myforexclass as mf
import datetime

start_year = 2024
start_month = 3
end_year = 2024
end_month = 6
interval = "1h"
tickers=["ETH-USD"]
result_data = mf.pd.DataFrame(columns=["date","ticker","return_hold","short_sma","long_sma","return_sma","short_ema","long_ema","return_ema",
                                       "short_dema","long_dema","return_dema","period_rsi","m_down_rsi","m_up_rsi"])

# حلقه برای دریافت داده‌های هر ماه
year = start_year
month = start_month
for ticker in tickers :
    while year < end_year or (year == end_year and month <= end_month):
        start_month_date = datetime.date(year, month, 1)
        if month == 12:
            end_month_date = datetime.date(year+1, 1, 1) - datetime.timedelta(days=1)
            year += 1
            month = 1
        else:
            end_month_date = start_month_date.replace(month=month+1) - datetime.timedelta(days=1)
            month += 1

        # نمایش نوار پیشرفت
        print(f" دریافت داده‌های ماه قبل {month}-{year} نماد {ticker}")
        t = mf.forex_backtest_class([ticker],start_month_date , end_month_date , interval )
        
        #******************************** SMA
        (s_sma,l_sma)= t.best_param_sma(ticker)
        r_sma=t.sma(ticker,s_sma,l_sma)
        r_h=t.get_perf_hold(-1)
        #******************************* EMA
        (s_ema,l_ema)= t.best_param_ema(ticker)
        r_ema=t.ema(ticker,s_ema,l_ema)
        #******************************* DMA
        (s_dema,l_dema)= t.best_param_dema(ticker)
        r_dema=t.dema(ticker,s_dema,l_dema)
        #******************************* RSI
        (p_rsi,d_rsi,u_rsi)= t.best_param_rsi(ticker)
        r_rsi=t.rsi(ticker,p_rsi,d_rsi,u_rsi)
        
        
        
        
        
        
        #new_row = mf.pd.Series([end_month_date,ticker ,r_h,s_sma, l_sma, r_sma,s_ema,l_ema,r_ema,s_dema,l_dema,r_dema,p_rsi,d_rsi,u_rsi], index=result_data.columns)
        #result_data = mf.pd.concat([result_data, new_row.to_frame().T], ignore_index=True)

result_data.set_index('date', inplace=True)
print(result_data)

