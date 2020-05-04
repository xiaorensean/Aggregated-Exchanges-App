import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

url = "https://api.pro.coinbase.com"

def get_time():
    time_endpoint = url + "/time"
    response = requests.get(time_endpoint)
    data = response.json()
    return data

def get_currency():
    currency_endpoint = url + "/currency"
    response = requests.get(currency_endpoint)
    data = response.json()
    return data

def get_market_stats(symbol):
    currency_endpoint = url + "/products/{}/stats".format(symbol)
    response = requests.get(currency_endpoint)
    data = response.json()
    return data

def get_tickers():
    tickers_endpoint = url + "/products"
    response = requests.get(tickers_endpoint)
    data = response.json()
    return data

def get_tickers_info(product_id):
    tickers_endpoint = url + "/products/{}/ticker".format(product_id)
    response = requests.get(tickers_endpoint)
    data = response.json()
    return data

def get_orderbook(product_id):
    orderbook_endpoint = url + "/products/{}/book?level=2".format(product_id)
    response = requests.get(orderbook_endpoint)
    data = response.json()
    return data

def get_trades(product_id):
    trade_endpoint = url + "/products/{}/trades".format(product_id)
    response = requests.get(trade_endpoint)
    data = response.json()
    return data

def get_historical_price(product_id):
    hist_price_endpoint = url + "/products/{}/candles".format(product_id)
    response = requests.get(hist_price_endpoint)
    data = response.json()
    return data

def get_market_summary(product_id):
    market_summary_endpoint = url + "/products/{}/stats".format(product_id)
    response = requests.get(market_summary_endpoint)
    data = response.json()
    return data

def get_custody():
    custody_endpoint = "https://api.custody.coinbase.com/api/marketing/currencies"
    response = requests.get(custody_endpoint)
    data = response.json()
    return data


if __name__ == "__main__":
    tickers = get_tickers()
    orderbook = get_orderbook("KNC-BTC")
    trades = get_trades("KNC-USD")
    custody = get_market_custody()