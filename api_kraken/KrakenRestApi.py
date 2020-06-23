import requests


BASE_URL = "https://api.kraken.com"

def get_asset_pairs_info():
    endpoint = "https://api.kraken.com/0/public/AssetPairs"
    response = requests.get(endpoint)
    try:
        data = response.json()['result']
    except:
        data = None
    return data


def get_tickers(symbol):
    endpoint = BASE_URL + "/0/public/Ticker?pair={}".format(symbol)
    response = requests.get(endpoint)
    try:
        data = response.json()['result']
    except:
        data = None
    
    return data

if __name__ == "__main__":
    tickers = [get_asset_pairs_info()[i]['altname'] for i in get_asset_pairs_info()]
    print(tickers)

