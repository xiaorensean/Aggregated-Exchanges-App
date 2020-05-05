import requests
import time
import os
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
dm_dir = os.path.dirname(current_dir)
pkg_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2


host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()


measurement = "exchange_open_interest"


def get_swap_tickers(base_token):
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "perpetual/pc/public/contracts/tickers?type={}".format(base_token)
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['data']
    return data

def swap_usd_update(swap_data,measurement):
    for sd in swap_data:
        fields = {}
        usd_oi = float(sd['holdAmount']) * float(sd['unitAmount'])
        coin_oi = float(sd['holdAmount'])/(float(sd['volume'])/float(sd['coinVolume']))
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"coin_denominated_symbol":sd['coinName']})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        tags = {}
        tags.update({"contract_symbol":sd['contract']})
        tags.update({"contract_exchange":"Okex"})
        dbtime = False
        host_1.write_points_to_measurement(measurement,dbtime,tags,fields)

def swap_usdt_update(swap_data,measurement):
    for sd in swap_data:
        fields = {}
        coin_oi = float(sd['holdAmount']) * float(sd['unitAmount'])
        usd_oi = coin_oi * float(sd['close'])
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"coin_denominated_symbol":sd['coinName']})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        tags = {}
        tags.update({"contract_symbol":sd['contract']})
        tags.update({"contract_exchange":"Okex"})
        dbtime = False
        host_1.write_points_to_measurement(measurement,dbtime,tags,fields)

def subscribe_swap_ticker(measurement):
    usd_swap = get_swap_tickers("USD")
    swap_usd_update(usd_swap,measurement)
    usdt_swap = get_swap_tickers("USDT")  
    swap_usdt_update(usdt_swap,measurement)



if __name__ == '__main__':
    subscribe_swap_ticker(measurement)
    while True:
        time.sleep(55)
        subscribe_swap_ticker(measurement)
