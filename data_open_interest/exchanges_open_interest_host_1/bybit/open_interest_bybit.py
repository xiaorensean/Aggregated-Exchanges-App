import time
import os
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
dm_dir = os.path.dirname(current_dir)
pkg_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from api_bybit.bybitRestApi import tickers_info

host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()


measurement = "exchange_open_interest"

def subscribe_open_interest(measurement):
    data = tickers_info()    
    for d in data:
        fields = {}
        if d['symbol'][-3:] == "USD":
            usd_oi = float(d['open_interest'])
            coin_oi = usd_oi/float(d['last_price'])
        else:
            coin_oi = float(d['open_interest'])
            usd_oi = coin_oi * float(d['last_price'])
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"coin_denominated_symbol":d['symbol'][:3]})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        tags = {}
        tags.update({"contract_symbol":d['symbol']})
        tags.update({"contract_exchange":"Bybit"})
        db_time = False
        host_1.write_points_to_measurement(measurement,db_time,tags,fields)


if __name__ == '__main__':
    subscribe_open_interest(measurement)
    while True:
        time.sleep(55)
        subscribe_open_interest(measurement)

