import pandas as pd
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

host = 'https://api.hbdm.com/api/v1/'


def get(endpoint,params=""):
    url = "{}/{}{}".format(host,endpoint,params)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print (response.status_code, response.json())


######## Public Data
def contract_info(symbol=None,contract_type=None):
    endpoint = "contract_contract_info" 
    if symbol is not None and contract_type is not None:
        params = "?symbol={}&contract_type={}".format(symbol,contract_type)
    else:
        params = ""
    contract_info = get(endpoint,params)
    contracts = contract_info['data']
    return contracts

def contract_open_interest(symbol=None,contract_type=None):
    endpoint = "contract_open_interest"
    if symbol is not None and contract_type is not None:
        params = "?symbol={}&contract_type={}".format(symbol,contract_type)
    else:
        params = ""
    contract_open_interest = get(endpoint,params)
    contracts = contract_open_interest['data']
    return contracts

def volume_report_huobidm(symbol):
    suffix = ["_CW","_NW","_CQ"]
    symbols = [symbol+s for s in suffix]
    symbols_vol = {}
    vol = 0
    for symb in symbols:
        response = requests.get("https://api.hbdm.com/market/detail/merged?symbol={}".format(symb))
        resp =  response.json()
        data = resp['tick']
        vol_contract = float(data['vol'])
        ask = data['ask'][0]*data['ask'][1]
        bid = data['bid'][0]*data['bid'][1]
        vwap = (ask+bid)/(data['bid'][1]+data['ask'][1])
        vol_usd = vol_contract*vwap
        symbols_vol.update({symb:vol_usd})
        vol += vol_usd
    symbols_vol.update({"total_huobidm_"+symbol.lower():vol})
    return symbols_vol

if __name__ == '__main__':
    all_contract = volume_report_huobidm('BSV')
