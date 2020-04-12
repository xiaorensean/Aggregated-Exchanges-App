import time
import os
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
dm_dir = os.path.dirname(current_dir)
pkg_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from api_deribit.DeribitRestApi import RestClient

host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()

deribit_api = RestClient()

measurement = "exchange_open_interest"


# Deribit
contract_names = [i['instrumentName'] for i in deribit_api.getinstruments()]
btc_contract_names = [i for i in contract_names if "BTC" in i]
eth_contract_names = [i for i in contract_names if "ETH" in i]

def write_open_interest_data(measurement):
    for cn in btc_contract_names:
        fields = {}
        # perpetual swap 
        if "PERPETUAL" in cn:
            summary_data = deribit_api.getsummary(cn)
            coin_oi = summary_data['openInterest']
            usd_oi = summary_data['openInterest'] * summary_data['last']
        # options 
        elif cn[-2:] == "-C" or cn[-2:] == "-P":
            summary_data = deribit_api.getsummary(cn)
            coin_oi = summary_data['openInterest']
            usd_oi = summary_data['openInterest'] * summary_data['uPx']
        # futures contract
        else:
            if "BTC" in cn:
                summary_data = deribit_api.getsummary(cn)
                usd_oi = summary_data['openInterestAmount']
                coin_oi = summary_data['openInterest']/(summary_data['volume']/summary_data['volumeBtc'])
            elif "ETH" in cn:
                summary_data = deribit_api.getsummary(cn)
                usd_oi = summary_data['openInterestAmount']
                coin_oi = summary_data['openInterest']/(summary_data['volume']/summary_data['volumeEth'])
            else:
                pass
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        if "BTC" in cn:
            fields.update({"coin_denominated_symbol":"BTC"})
        elif "ETH" in cn:
            fields.update({"coin_denominated_symbol":"ETH"})
        else:
            pass
        tags = {}
        tags.update({"contract_symbol":cn})
        tags.update({"contract_exchange":"Deribit"})
        dbtime = False
        host_2.write_points_to_measurement(measurement, dbtime, tags, fields)



def subscribe_open_interest(measurement):
    write_open_interest_data(measurement)
    while True:
        time.sleep(60)
        write_open_interest_data(measurement)


if __name__ == "__main__":
    subscribe_open_interest(measurement)