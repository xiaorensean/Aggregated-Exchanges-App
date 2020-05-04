import requests

base_url_future =  "https://fapi.binance.com/"
base_url_spot_ajax = "https://www.binance.com/api/"
base_url_future_ajax = "https://www.binance.com/fapi/"
base_url_spot = "https://api.binance.com"

def get_exchange_info_future_stats():
    endpoint = base_url_future_ajax+"v1/exchangeInfo"
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_exchange_info():
    endpoint = base_url_spot_ajax+"v3/exchangeInfo"
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_futures_tickers():
    endpoint = base_url_future_ajax + 'v1/ticker/24hr' 
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_future_stats_open_interest(symbol,period,start_ts=None,end_ts=None):
    if start_ts is not None and end_ts is not None:
        endpoint = base_url_future + "futures/data/openInterestHist?symbol={}&period={}&limit=500&startTime={}&endTime={}".format(symbol,period,start_ts,end_ts)
    else:
        endpoint = base_url_future + "futures/data/openInterestHist?symbol={}&period={}&limit=500".format(symbol,period)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_future_stats_long_short_ratio_global(symbol,period,start_ts=None,end_ts=None):
    if start_ts is not None and end_ts is not None:
        endpoint = base_url_future + "futures/data/globalLongShortAccountRatio?symbol={}&period={}&limit=500&startTime={}&endTime={}".format(symbol,period,start_ts,end_ts)
    else:
        endpoint = base_url_future + "futures/data/globalLongShortAccountRatio?symbol={}&period={}&limit=500".format(symbol,period)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_future_stats_long_short_ratio_position(symbol,period,start_ts=None,end_ts=None):
    if start_ts is not None and end_ts is not None:
        endpoint = base_url_future + "futures/data/topLongShortPositionRatio?symbol={}&period={}&limit=500&startTime={}&endTime={}".format(symbol,period,start_ts,end_ts)
    else:
        endpoint = base_url_future + "futures/data/topLongShortPositionRatio?symbol={}&period={}&limit=500".format(symbol,period)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_future_stats_long_short_ratio_account(symbol,period,start_ts=None,end_ts=None):
    if start_ts is not None and end_ts is not None:
        endpoint = base_url_future + "futures/data/topLongShortAccountRatio?symbol={}&period={}&limit=500&startTime={}&endTime={}".format(symbol,period,start_ts,end_ts)
    else:
        endpoint = base_url_future + "futures/data/topLongShortAccountRatio?symbol={}&period={}&limit=500".format(symbol,period)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_future_stats_taker_buy_sell_volume(symbol,period,start_ts=None,end_ts=None):
    if start_ts is not None and end_ts is not None:
        endpoint = base_url_future + "futures/data/takerLongShortRatio?symbol={}&period={}&limit=500&startTime={}&endTime={}".format(symbol,period,start_ts,end_ts)
    else:
        endpoint = base_url_future + "futures/data/takerLongShortRatio?symbol={}&period={}&limit=500".format(symbol,period)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_spot24(symbol):
    url = base_url_spot + "/api/v3/ticker/24hr?symbol={}".format(symbol)
    response = requests.get(url)
    data = response.json()
    return data

def post_open_interest(symbol,period):
    oi_endpoint = "https://www.binance.com/gateway-api/v1/public/future/data/open-interest-stats"
    data = {"name":symbol,
             "periodMinutes":period}
    response = requests.post(oi_endpoint,json=data)
    data = response.json()
    return data
    
def post_long_short_ratio_level(symbol,period):
    lsr_endpoint = "https://www.binance.com/gateway-api/v1/public/future/data/global-long-short-account-ratio"
    data = {"name":symbol,
             "periodMinutes":period}
    response = requests.post(lsr_endpoint,json=data)
    data = response.json()
    return data

def post_long_short_ratio_position(symbol,period):
    lsrp_endpoint = "https://www.binance.com/gateway-api/v1/public/future/data/long-short-position-ratio"
    data = {"name":symbol,
             "periodMinutes":period}
    response = requests.post(lsrp_endpoint,json=data)
    data = response.json()
    return data

def post_long_short_ratio_account(symbol,period):
    lsrp_endpoint = "https://www.binance.com/gateway-api/v1/public/future/data/long-short-account-ratio"
    data = {"name":symbol,
             "periodMinutes":period}
    response = requests.post(lsrp_endpoint,json=data)
    data = response.json()
    return data

def post_taker_buy_sell_volume(symbol,period):
    lsrp_endpoint = "https://www.binance.com/gateway-api/v1/public/future/data/taker-long-short-ratio"
    data = {"name":symbol,
             "periodMinutes":period}
    response = requests.post(lsrp_endpoint,json=data)
    data = response.json()
    return data

def get_open_interest(symbol):
    oi_endpoint = base_url_future+"fapi/v1/openInterest?symbol={}".format(symbol)
    response = requests.get(oi_endpoint)
    data = response.json()
    return data

def get_liquidation_orders(symbol,start_ts=None,end_ts=None):
    if start_ts == None and end_ts == None:
        liquidation_endpoint = base_url_future+\
        "fapi/v1/allForceOrders?symbol={}&limit=1000".format(symbol)
    else:
        liquidation_endpoint = base_url_future+\
        "fapi/v1/allForceOrders?symbol={}&startTime={}&endTime={}&limit=1000".format(symbol,start_ts,end_ts)
    response = requests.get(liquidation_endpoint)
    data = response.json()
    return data

def get_trades(symbol,market_type):
    if market_type == "f":
        base_url = base_url_future + "fapi/"
    elif market_type == "s":
        base_url = base_url_spot_ajax
    else:
        return
    trade_endpoint = base_url + "v1/trades?symbol={}&limit=1000".format(symbol)
    response = requests.get(trade_endpoint)
    data = response.json()
    return data

def get_orderbook(symbol,market_type):
    if market_type == "f":
        base_url = base_url_future + "fapi/"
    elif market_type == "s":
        base_url = base_url_spot_ajax
    else:
        return
    ob_endpoint = base_url + "v1/depth?symbol={}&limit=100".format(symbol)
    response = requests.get(ob_endpoint)
    data = response.json()
    return data

def get_funding_rates(symbol,limit):
    fr_endpoint = base_url_future + "fapi/v1/fundingRate?symbol={}&limit={}".format(symbol,limit)
    response = requests.get(fr_endpoint)
    data = response.json()
    return data

if __name__ == "__main__":
    future_tickers = get_exchange_info_future_stats()
    data = get_future_stats_long_short_ratio_global("BTCUSDT",'5m',1586132700000-60*60*24*1000,1586132700000)
    a = get_trades("SOLBUSD","s")
    #data = get_future_stats_taker_buy_sell_volume("BTCUSDT",'5m',1586132700000-60*60*24*1000,1586132700000)
    #a = post_long_short_ratio_position("EOSUSDT",5)['data']
    #a1 = post_long_short_ratio_account("EOSUSDT",5)['data']
    #a2 = post_long_short_ratio_level("EOSUSDT",5)['data']
    #liquidation_data =  get_liquidation_orders()
    #future_oi = post_open_interest("ETHUSDT",5)
    #future_lsr = post_long_short_account_ratio("ETHUSDT",5)