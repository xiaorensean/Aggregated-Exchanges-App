import time
import os
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
dm_dir = os.path.dirname(current_dir)
pkg_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from api_bitmex.BitmexRestApi import get_instrument
from influxdb_client.influxdb_client_qa_host_1 import InfluxClientHostQA1

host_1 = InfluxClientHostQA1()
host_2 = InfluxClientHost2()

measurement = "exchange_open_interest"



# Bitmex Futures Open Interest
def write_open_interest_data(measurement):
    all_instrument = get_instrument()
    for ai in all_instrument:
        fields = {}
        usd_oi = ai['openInterest']
        coin_oi = ai['openInterest']/ai['lastPrice']
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"coin_denominated_symbol":ai["underlying"]})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"contract_symbol":ai['symbol']})
        tags.update({"contract_exchange":"Bitmex"})
        dbtime = False
        host_1.write_points_to_measurement(measurement, dbtime, tags, fields)


def subscribe_open_interest(measurement):
    write_open_interest_data(measurement)
    while True:
        time.sleep(55)
        write_open_interest_data(measurement)


if __name__ == "__main__":
    subscribe_open_interest(measurement)
