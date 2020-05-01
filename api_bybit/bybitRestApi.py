import requests

base_url = "https://api.bybit.com"
tickers_url = "/v2/public/tickers"
trades_url = "/v2/public/trading-records"

# request method
def request_get(endpoint):
    response = requests.get(endpoint)
    resp = response.json()
    if resp['ret_msg'] == "OK":
        data = resp['result']
    else:
        print(resp['ret_msg'])
        return 
    current_time = resp['time_now']
    for d in data:
        d.update({'current_snapshot':current_time})
    return data


# not the official api 
def funding_rates(limit):
    funding_rates_endpoint = 'https://api2.bybit.com/funding-rate/list?symbol=&date=&export=false&page=1&limit={}'.format(limit)
    response = requests.get(funding_rates_endpoint)
    resp = response.json()
    if resp['ret_msg'] == "ok":
        data = resp['result']
    else:
        print("Error when fetching")
    return data


# tickers info 
def tickers_info():
    tickers_endpoint = base_url + tickers_url
    data = request_get(tickers_endpoint)
    return data

def trades(ticker):
    trades_endpoint = base_url + trades_url + "?symbol={}&limit=1000".format(ticker) 
    data = request_get(trades_endpoint)
    return data

def trades_usdt(ticker):
    trades_endpoint = base_url + "/public/linear/recent-trading-records?symbol={}&limit=1000".format(ticker)
    data = request_get(trades_endpoint)
    return data

if __name__ == "__main__":
    trade_inverse = trades("BTCUSDT")
    trade_usdt = trades_usdt("BTCUSDT")
    fr = funding_rates(100)