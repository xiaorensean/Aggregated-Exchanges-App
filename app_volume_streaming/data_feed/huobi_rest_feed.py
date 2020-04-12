import pandas as pd
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# request get method 
def data_fetch(endpoint):
    response = requests.get(endpoint)
    data = response.json()
    return data

# huobi api 
def get_related_ticker_huobi(symbol_related):
    ticker_endpoint = "https://api.huobi.pro/market/tickers"
    ticker = data_fetch(ticker_endpoint)
    ticker_data = ticker['data']
    bsv_tickers = []
    for td in ticker_data:
        if symbol_related in td['symbol']:
            bsv_tickers.append(td['symbol'])
        else:
            pass
    return bsv_tickers

def get_volume(symbol):
    ms_endpoint = "https://api.huobi.pro/market/detail?symbol={}"
    ms_endpoint = ms_endpoint.format(symbol)
    market_summary = data_fetch(ms_endpoint)
    data = market_summary['tick']
    # volume based on ticker
    volume = data['amount']
    return volume

def get_vwap_price(symbol):
    trade_endpoint = "https://api.huobi.pro/market/trade?symbol={}"
    trade_endpoint = trade_endpoint.format(symbol)
    trade = data_fetch(trade_endpoint)
    trade_data = trade['tick']['data']
    if len(trade_data) == 1:
        price = trade_data[0]['price']
    else:
        dollar_value = 0
        total_amount = 0
        for td in trade_data:
            dollar_value += td['amount'] + td['price']
            total_amount += td['amount']
        price = dollar_value/total_amount 
    return price

def volume_report_huobi(related_ticker): 
    ticker_usdt = related_ticker + 'usdt'
    tickers = get_related_ticker_huobi(related_ticker)
    volume_total = 0
    volume_base = {}
    for t in tickers:
        volume = get_volume(t)
        volume_total += volume
        volume_base.update({t:volume})
    volume_base.update({'total_huobi_'+related_ticker.lower():volume_total})
    price_usdt = get_vwap_price(ticker_usdt)
    volume_usdt = {ticker:volume_base[ticker]*price_usdt for ticker in volume_base}
    volume_usdt_df = pd.DataFrame(volume_usdt,index=["Huobi"])
    return volume_usdt

if __name__ == "__main__":
    bsv_volume = volume_report_huobi('bsv')
    bch_volume = volume_report_huobi('bch')


