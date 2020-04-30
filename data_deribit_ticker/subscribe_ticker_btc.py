import time
import os 
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
import multiprocessing
import json
from time import sleep

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from api_deribit.DeribitRestApi import RestClient

db = InfluxClientHost1()
deribit = RestClient()
#measurement = "deribit_ticker_all_symbol"
measurement = "test_deribit_ticker"

def symbol_btc_cluster(num):
    symbols = [i['instrumentName'] for i in deribit.getinstruments()]
    btc_symbols = [symb for symb in symbols  if "BTC" in symb]
    clusters = str(len(btc_symbols)/num)
    integ = int(clusters.split(".")[0])
    ss = []
    for i in range(integ+1):
        s = btc_symbols[num*i:num*(i+1)]
        ss.append(s)
    return ss

#eth_symbols = [symb for symb in symbols  if "ETH" in symb]
num = 200
symbols_clus = symbol_btc_cluster(num)
print(len(symbols_clus))
def write_ticker_data(measurement,d):
    fields = {}
    try:
        fields.update({"askPrice":float(d["askPrice"])})
    except: 
        fields.update({"askPrice":None})
    try: 
        fields.update({"bidPrice":float(d["bidPrice"])})
    except:
        fields.update({"bidPrice":None})
    fields.update({"timestamp":d["created"]})
    try:
        fields.update({"high":float(d["high"])})
    except:
        fields.update({"high":None})
    try:
        fields.update({"IR":float(d["IR"])})
    except:
        fields.update({"IR":None})
    try:
        fields.update({"last":float(d["last"])})
    except:
        fields.update({"last":None})
    try:
        fields.update({"low":float(d["low"])})
    except:
        fields.update({"low":None})
    try:
        fields.update({"markPrice":float(d["markPrice"])})
    except:
        fields.update({"markPrice":None})
    try:
        fields.update({"midPrice":float(d["midPrice"])})
    except:
        fields.update({"midPrice":None})
    try:
        fields.update({"open_interest":float(d["openInterest"])})
    except:
        fields.update({"open_interest":None})
    fields.update({"uIx":d["uIx"]})
    try:
        fields.update({"uPx":float(d["uPx"])})
    except:
        fields.update({"uPx":None})
    try:
        fields.update({"volume":float(d["volume"])})
    except:
        fields.update({"volume":None})
    try:
        fields.update({"volume_btc":float(d["volumeBtc"])})
    except:
        fields.update({"volume_btc":None})
    tags = {}
    tags.update({"instrument_name":d["instrumentName"]})
    dbtime = False
    db.write_points_to_measurement(measurement, dbtime, tags, fields)


# get tickers
a = time.time()
datas = []
for s in symbols_clus[0]:
    try:
        data = deribit.getsummary(s)
        write_ticker_data(measurement, data)
    except Exception as err:
        print(err)
a1 = time.time()
print(a1-a)



