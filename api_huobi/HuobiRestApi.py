import requests



host = 'https://api.hbdm.com/api/v1/'
host_market = 'https://api.hbdm.com/market'
swap_market = "https://api.hbdm.com/"

def get(endpoint,params="",market=None):
    if market is None:
        url = "{}/{}{}".format(host,endpoint,params)
    else:
        url = "{}/{}{}".format(host_market,endpoint,params)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print (response.status_code, response.json())


######## Public Data
#### Swap Data Endpoint
def get_swap_info():
    endpoint = swap_market + "swap-api/v1/swap_contract_info"
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_index_price():
    endpoint = swap_market + "swap-api/v1/swap_index"
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_price_limit(symbol):
    endpoint = swap_market + "swap-api/v1/swap_price_limit?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_open_interest(symbol):
    endpoint = swap_market + "swap-api/v1/swap_open_interest?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_depth(symbol):
    endpoint = swap_market + "swap-ex/market/depth?contract_code={}&type=step0".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_kline_data(symbol,period,size):
    endpoint = swap_market + "swap-ex/market/history/kline?contract_code={}&period={}&size={}".format(symbol,period,size)
    response = requests.get(endpoint)
    data = response.json()
    return data
    
def get_swap_market_data_overview(symbol):
    endpoint = swap_market + "swap-ex/market/detail/merged?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_trades(symbol):
    endpoint = swap_market + "swap-ex/market/trade?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_batch_trades(symbol,size):
    endpoint = swap_market + "swap-ex/market/history/trade?contract_code={}&size={}".format(symbol,size)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_market_risk_info():
    endpoint = swap_market + "swap-api/v1/swap_risk_info"
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_insurance_fund():
    endpoint = swap_market + "swap-api/v1/swap_insurance_fund"
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_hist_open_interets(symbol):
    endpoint = swap_market + "swap-api/v1/hist_open_interest?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_liquidation_orders(symbol):
    endpoint = swap_market + "swap-api/v1/swap_liquidation_orders?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_funding_rate(symbol):
    endpoint = swap_market + "swap-api/v1/swap_funding_rate?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_swap_hist_funding_rate(symbol):
    endpoint = swap_market + "swap-api/v1/swap_historical_funding_rate?contract_code={}".format(symbol)
    response = requests.get(endpoint)
    data = response.json()
    return data



#### Futures Data Endpoint 
def contract_info(symbol=None,contract_type=None):
    endpoint = "contract_contract_info" 
    if symbol is not None and contract_type is not None:
        params = "?symbol={}&contract_type={}".format(symbol,contract_type)
    else:
        params = ""
    contract_info = get(endpoint,params,None)
    return contract_info

def contract_open_interest(symbol=None,contract_type=None):
    endpoint = "contract_open_interest"
    if symbol is not None and contract_type is not None:
        params = "?symbol={}&contract_type={}".format(symbol,contract_type)
    else:
        params = ""
    contract_open_interest = get(endpoint,params,None)
    return contract_open_interest

def contract_open_interest_hist(symbol,contract_type,period,size,amount_type):
    endpoint = "contract_his_open_interest"
    if symbol is not None and contract_type is not None and period is not None and size is not None and amount_type is not None:
        params = "?symbol={}&contract_type={}&period={}&amount_type={}&size={}".format(symbol,contract_type,period,amount_type,size)
    else:
        params = ""
    contract_open_interest = get(endpoint,params,None)
    return contract_open_interest

def contract_index_price(symbol):
    endpoint = "contract_index"
    if symbol is not None:
        params = "?symbol={}".format(symbol)
    else:
        params = ""
    contract_index_price = get(endpoint,params,None)
    return contract_index_price

def contract_price_limit(symbol,contract_type):
    endpoint = "contract_price_limit"
    if symbol is not None and contract_type is not None:
        params = "?symbol={}&contract_type={}".format(symbol,contract_type)
    else:
        params = ""
    contract_price_limit = get(endpoint,params,None)
    return contract_price_limit

def contract_delivery_price(symbol):
    endpoint = "contract_delivery_price"
    if symbol is not None:
        params = "?symbol={}".format(symbol)
    else:
        params = ""
    contract_delivery_price = get(endpoint,params,None)
    return contract_delivery_price

def contract_risk_info(symbol):
    endpoint = "contract_risk_info"
    if symbol is not None:
        params = "?symbol={}".format(symbol)
    else:
        params = ""
    contract_risk_info = get(endpoint,params,None)
    return contract_risk_info

def contract_insurance_fund(symbol):
    endpoint = "contract_insurance_fund"
    if symbol is not None:
        params = "?symbol={}".format(symbol)
    else:
        params = ""
    contract_insurance_fund = get(endpoint,params,None)
    return contract_insurance_fund

def contract_adjust_factor(symbol):
    endpoint = "contract_adjustfactor"
    if symbol is not None:
        params = "?symbol={}".format(symbol)
    else:
        params = ""
    contract_adjust_factor = get(endpoint,params,None)
    return contract_adjust_factor

def contract_elite_account_ratio(symbol,period):
    endpoint = "contract_elite_account_ratio"
    if symbol is not None:
        params = "?symbol={}&period={}".format(symbol,period)
    else:
        params = ""
    contract_adjust_factor = get(endpoint,params,None)
    return contract_adjust_factor

def contract_elite_position_ratio(symbol,period):
    endpoint = "contract_elite_account_ratio"
    if symbol is not None:
        params = "?symbol={}&period={}".format(symbol,period)
    else:
        params = ""
    contract_adjust_factor = get(endpoint,params,None)
    return contract_adjust_factor
 

def market_depth(symbol,step):
    endpoint = "depth"
    if symbol is not None:
        params = "?symbol={}&type={}".format(symbol,step)
    else:
        params = ""
    contract_depth = get(endpoint,params,'market')
    return contract_depth

def market_kline(period,size,symbol):
    endpoint = "history/kline"
    if symbol is not None and period is not None and size is not None:
        params = "?period={}&size={}&symbol".format(period,size,symbol)
    else:
        params = ""
    market_kline = get(endpoint,params,'market')
    return market_kline

def market_overview(symbol):
    endpoint = "detail/merged"
    if symbol is not None:
        params = "?symbol={}".format(symbol)
    else:
        params = ""
    market_overview = get(endpoint,params,'market')
    return market_overview
    
def market_trade(symbol):
    endpoint = "trade"
    if symbol is not None:
        params = "?symbol={}".format(symbol)
    else:
        params = ""
    market_trade = get(endpoint,params,'market')
    return market_trade
    
def market_trade_batch(symbol,size):
    endpoint = "history/trade"
    if symbol is not None:
        params = "?symbol={}&size={}".format(symbol,size)
    else:
        params = ""
    market_trade = get(endpoint,params,'market')
    return market_trade
    

if __name__ == "__main__":
    #a = contract_price_limit('BTC','this_week')
    #a = contract_open_interest_hist("BTC","this_week","60min",200,1)
    data = get_swap_info()
    #data = get_swap_open_interest("BTC-USD")