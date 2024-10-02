import Coinex_API_Class as CoinexAPI
    
api_key = "47D7C4B286224298BB3D88A9D7161A45"
api_secret = "F0F2D4156F6892ED2F03576FEE74CFA658C5F8271DCE102D"
coinex = CoinexAPI.Coinex_API(api_key,api_secret)

coinex.save_filtered_spot_market()