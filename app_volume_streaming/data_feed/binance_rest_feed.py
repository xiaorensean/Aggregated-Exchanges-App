import pandas as pd
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

base_url_spot_ajax = "https://www.binance.com/api/"

def get_tickers(related_symbol ):
    endpoint = base_url_spot_ajax+"v3/exchangeInfo"
    response = requests.get(endpoint)
    data = response.json()
    symbols = data['symbols']
    all_symbol = []
    for symb in symbols:
        if (related_symbol in symb['symbol']) and (symb['status'] == 'TRADING'):
            all_symbol.append(symb['symbol'])
        else:
            pass
    return all_symbol

def get_volume_binance(related_symbol):
    endpoint = base_url_spot_ajax + 'v1/ticker/24hr' 
    response = requests.get(endpoint)
    data = response.json()
    volume = {}
    volume_total = 0
    for d in data:
        if (related_symbol in d['symbol']) and (d['priceChange'] != '0.00000000'): 
            volume.update({d['symbol']:float(d['quoteVolume'])})
            volume_total += float(d['quoteVolume'])
        else:
            pass
    volume.update({"total_binance_"+related_symbol.lower():volume_total})
    return volume


if __name__ == "__main__":
    volume_bch = get_volume_binance('BCH')