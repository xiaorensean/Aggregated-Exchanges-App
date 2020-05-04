import requests


BASE_URL = "https://api.kraken.com"

def get_tickers(symbol):
    endpoint = BASE_URL + "/0/public/Ticker?pair={}".format(symbol)
    response = requests.get(endpoint)
    try:
        data = response.json()['result']
    except:
        data = None
    
    return data





if __name__ == "__main__":
    symbol = "XETHXXBT"
    data = get_tickers(symbol)    
