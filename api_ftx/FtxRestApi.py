from urllib.request import Request, urlopen
import json
import arrow
import time
import datetime
import pandas as pd


# FTX base URL
base_URL = 'https://ftx.com/api'


# get json 
def api_json(url):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(url,headers=hdr)
    response = urlopen(req)
    page = response.read().decode('utf-8')
    data = json.loads(page)
    if data['success']:
        json_format = data['result']
    else:
        counter = 0
        while counter <=4:
            counter += 1
            time.sleep(10)
            response = urlopen(req)
            page = response.read().decode('utf-8')
            data = json.loads(page)
            if data['success']:
                json_format = data['result']
                break 
            return json_format
    return json_format


# list all tickers 
def get_all_tickers():
    tickers_url = base_URL + "/markets"    
    response = api_json(tickers_url)
    all_tickers = [i['name'] for i in response]
    return all_tickers    

# list all contract names
def get_contract_names():
    futures_list_url = base_URL + '/futures' 
    source_data = api_json(futures_list_url)
    future_expiry = {}
    for data in source_data:
        if data['expiry'] is not None:
            future_expiry.update({data['name']:data['expiry']})
    futures_list = [fl['name'] for fl in api_json(futures_list_url) if fl['expiry'] is not None]
    perp_list = [fl['name'] for fl in api_json(futures_list_url) if fl['expiry'] is None]
    contract_list = [fl['name'] for fl in api_json(futures_list_url)]
    return(contract_list)


# get trade 
def get_trades(market_name,start_time,end_time):
    trade_url = base_URL + '/markets/{}/trades?limit=100&start_time={}&end_time={}'.format(market_name,start_time,end_time)
    trade_data = api_json(trade_url)
    return trade_data

# get orderbook
def get_orderbook(market_name,depth=100):
    orderbook_url = base_URL + '/markets/{}/orderbook?depth={}'.format(market_name,depth)
    orderbook_data = api_json(orderbook_url)
    return orderbook_data

# get funding rates
def get_funding_rates():
    funding_rates_url = base_URL + '/funding_rates'
    funding_rates_data = api_json(funding_rates_url)
    return funding_rates_data

# get futures stats
def get_futures_stats(contract_name):
    future_stats_url = base_URL + '/futures/{}/stats'.format(contract_name)
    future_stats_data = api_json(future_stats_url)
    return future_stats_data

# get future
def get_future(contract_name):
    future_stats_url = base_URL + '/futures/{}'.format(contract_name)
    future_stats_data = api_json(future_stats_url)
    return future_stats_data

if __name__ == '__main__':
    future_contracts = get_contract_names()
    #for future_contract in future_contracts:
    #    data_entry = get_futures_stats(future_contract)
    move_contracts = [fc for fc in future_contracts if "MOVE" in fc]
    data = get_future("ADA-PERP")
    #mc_ob = []
    #for mc in move_contracts:
    #    mc_ob.append(get_orderbook(mc))



