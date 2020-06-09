import multiprocessing as mp
import time
import os
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
dm_dir = os.path.dirname(current_dir)
pkg_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(pkg_dir)
from api_binance.BinanceRestApi import get_open_interest,get_futures_tickers
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from influxdb_client.influxdb_client_qa_host_1 import InfluxClientHostQA1

host_1 = InfluxClientHostQA1()
host_2 = InfluxClientHost2()


measurement = "exchange_open_interest"

# Binance Futures Contract
contract_names = {i["symbol"]:float(i["quoteVolume"])/float(i["volume"]) for i in get_futures_tickers()}


# Binance Futures Open Interets
def write_open_interest_data(symbol,rate,measurement):
    fields = {}
    coin_oi = float(get_open_interest(symbol)['openInterest'])
    usd_oi = rate * coin_oi
    fields.update({"coin_denominated_open_interest":float(coin_oi)})
    fields.update({"coin_denominated_symbol":symbol[:-4]})
    fields.update({"usd_denominated_open_interest":float(usd_oi)})
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    tags.update({"contract_symbol":symbol})
    tags.update({"contract_exchange":"Binance"})
    dbtime = False
    host_1.write_points_to_measurement(measurement, dbtime, tags, fields)


def subscribe_open_interest(symbol,rate,measurement):
    write_open_interest_data(symbol,rate,measurement)
    while True:
        time.sleep(55)
        write_open_interest_data(symbol,rate,measurement)


if __name__ == "__main__":
    # all processes
    processes = {}
    for cn in contract_names:
        binance_oi = mp.Process(target=subscribe_open_interest,args=(cn,contract_names[cn],measurement))
        binance_oi.start()
        processes.update({cn:binance_oi})

