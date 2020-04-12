import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# request get method 
def data_fetch(endpoint):
    response = requests.get(endpoint)
    data = response.json()
    return data

currency_endpoint = "https://api.coinbase.com/v2/currencies"
currency = data_fetch(currency_endpoint)
bsv_related = []
for c in currency:
    if "ETH" in c:
        bsv_related.append(c['id'])
    else:
        pass
