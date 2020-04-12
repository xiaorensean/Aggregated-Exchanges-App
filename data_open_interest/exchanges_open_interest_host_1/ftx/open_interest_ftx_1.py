import time
import os
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
dm_dir = os.path.dirname(current_dir)
pkg_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from api_ftx.FtxRestApi import get_futures_stats, get_future, get_contract_names

host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()

measurement = "exchange_open_interest"

# FTX Futures Contract
contract_names = get_contract_names()
contract_names_half = contract_names[:int(len(contract_names)/2)]


# FTX Futures Open Interets
def write_open_interest_data(measurement):
    for symbol in contract_names_half:
        fields = {}
        coin_oi = get_futures_stats(symbol)['openInterest']
        usd_oi = get_future(symbol)['last'] * coin_oi
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"coin_denominated_symbol":get_future(symbol)["underlying"]})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        tags = {}
        tags.update({"contract_symbol":symbol})
        tags.update({"contract_exchange":"FTX"})
        dbtime = False
        host_1.write_points_to_measurement(measurement, dbtime, tags, fields)


def subscribe_open_interest(measurement):
    write_open_interest_data(measurement)
    while True:
        time.sleep(60)
        write_open_interest_data(measurement)


if __name__ == "__main__":
    subscribe_open_interest(measurement)
