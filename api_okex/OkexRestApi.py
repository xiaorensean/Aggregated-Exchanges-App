import requests
import os

current_dir = os.path.dirname(os.path.abspath(__file__))



def get_tickers():
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "futures/pc/market/marketOverview.do?symbol=f_usd_all"
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['ticker']
    all_tickers = list(set([d['symbolName'] for d in data]))
    symbol_usdt = ["f_usdt_"+i for i in all_tickers]
    symbol_usd = ["f_usd_"+i for i in all_tickers]
    return [symbol_usdt,symbol_usd]

def get_spot_tickers(symbol):
    endpoint = "https://www.okex.com/v2/spot/markets/tickers?symbol={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data
    

# fetch ticker info
def post_ticker_info(symbol):
    base = "https://www.okex.com/v2"
    oi_url = "/futures/pc/market/tickers.do"
    tickers = base + oi_url
    param = {"symbol":symbol}
    response = requests.post(tickers,data=param)
    resp = response.json()
    if resp["msg"] == "success":
        data = resp['data']
    return data


def get_swap_tickers(base_token):
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "perpetual/pc/public/contracts/tickers?type={}".format(base_token)
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['data']
    return data

if __name__ == "__main__":
    futures = get_tickers()
    a = get_spot_tickers("eth_btc")
