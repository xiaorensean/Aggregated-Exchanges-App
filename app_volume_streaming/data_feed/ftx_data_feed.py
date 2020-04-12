import requests
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# FTX base URL
base_URL = 'https://ftx.com/api'

# list all tickers 
def get_volume_ftx(related_symbol):
    tickers_url = base_URL + "/markets"    
    resp = requests.get(tickers_url)
    response = resp.json()
    data = response['result']
    related_symbol_info = []
    for d in data:
        if related_symbol in d['name']:
            related_symbol_info.append(d)
    volume_total = 0 
    volume_ticker = {}
    for rsi in related_symbol_info:
        try:
            vol = rsi['volumeUsd24h']
        except KeyError:
            pass
            vol = 0
        volume_total += vol
        volume_ticker.update({rsi['name']:vol})
    volume_ticker.update({"total_ftx_"+related_symbol.lower():volume_total})
    return volume_ticker  


if __name__ == "__main__":
    bsv_report = get_volume_ftx('BSV')
    bch_report = get_volume_ftx('BCH')
