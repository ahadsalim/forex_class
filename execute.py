import Coinex_API_Class as CoinexAPI

api_key = "47D7C4B286224298BB3D88A9D7161A45"
api_secret = "F0F2D4156F6892ED2F03576FEE74CFA658C5F8271DCE102D"

coinex = CoinexAPI.Coinex_API(api_key,api_secret)

df= prepare_data("15min",48)  # For 12 Hours ago
df.to_csv('data.csv', index=False)

df2 = df[(df['Recomandation'] == "STRONG_BUY") | (df['Recomandation'] == "STRONG_SELL")]
df2 = df2.sort_values(['Cum_Return'],ascending=False)
df2 = df2.reindex(['Time', 'period', 'limit','Recomandation','symbol','Cum_Return','Close','Volume','Value','Buy','Sell','Neutral'], axis=1)