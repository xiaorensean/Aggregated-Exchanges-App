import time
import os
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
dm_dir = os.path.dirname(current_dir)
pkg_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from api_huobi.HuobiRestApi import get_swap_info, get_swap_open_interest, \
                     contract_info, contract_open_interest, \
                     get_swap_index_price, contract_index_price    

host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()

measurement = "exchange_open_interest"


# Huobi Swap Open Interest
def write_open_interest_data_swap(measurement):
    swap_tickers = {i['contract_code']:i['contract_size'] for i in get_swap_info()['data']}
    for symb in swap_tickers:
        fields = {}
        coin_oi = get_swap_open_interest(symb)['data'][0]['amount']
        usd_oi = get_swap_open_interest(symb)['data'][0]['volume']*swap_tickers[symb]
        coin_symbol = get_swap_open_interest(symb)['data'][0]['symbol']
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"coin_denominated_symbol":coin_symbol})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"contract_symbol":symb})
        tags.update({"contract_exchange":"Huobi"})
        dbtime = False
        host_1.write_points_to_measurement(measurement, dbtime, tags, fields)


# Huobi Futures Open Interest
def write_open_interest_data_futures(measurement):
    futures_oi = contract_open_interest()['data']
    for foi in futures_oi:
        fields = {}
        coin_oi = foi['amount']
        if "BTC" in foi['contract_code']:
            usd_oi = foi['volume']*100
        else:
            usd_oi = foi['volume']*10
        coin_symbol = foi['symbol']
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"coin_denominated_symbol":coin_symbol})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        tags = {}
        tags.update({"contract_symbol":foi['contract_code']})
        tags.update({"contract_exchange":"Huobi"})
        dbtime = False
        host_1.write_points_to_measurement(measurement, dbtime, tags, fields)



def subscribe_open_interest_futures(measurement):
    write_open_interest_data_futures(measurement)
    while True:
        time.sleep(55)
        write_open_interest_data_futures(measurement)


if __name__ == "__main__":
    subscribe_open_interest_futures(measurement)
