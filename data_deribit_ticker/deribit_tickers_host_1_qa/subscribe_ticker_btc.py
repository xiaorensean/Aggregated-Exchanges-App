import traceback
import datetime
import time
import os 
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
import multiprocessing
import json
from time import sleep

sys.path.append(os.path.dirname(os.path.dirname(current_dir)))
from influxdb_client.influxdb_client_qa_host_1 import InfluxClientHostQA1
from api_deribit.DeribitRestApi import RestClient
from utility.error_logger_writer import logger


db = InfluxClientHostQA1()
deribit = RestClient()
measurement = "deribit_tickers"
#measurement = "test_deribit_ticker"

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

def symbol_btc():
    symbols = [i['instrumentName'] for i in deribit.getinstruments()]
    btc_symbols = [symb for symb in symbols  if "BTC" in symb]
    return btc_symbols


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
    try:
        fields.update({"uIx":d["uIx"]})
    except: 
        fields.update({"uIx":None})
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
    fields.update({"is_api_return_timestamp": True})
    tags = {}
    tags.update({"instrument_name":d["instrumentName"]})
    ts = " ".join(d['created'].split(" ")[:2])
    dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    uts = time.mktime(dt_temp.timetuple()) * 1000
    dt = datetime.datetime.utcfromtimestamp(uts / 1000)
    dbtime = dt
    db.write_points_to_measurement(measurement, dbtime, tags, fields)


# get tickers
def subscribe_tickers(measurement):
    btc_symbols = symbol_btc()
    for s in btc_symbols:
        try:
            data = deribit.getsummary(s)
            time.sleep(0.01)
            write_ticker_data(measurement, data)
        except Exception as err:
            error = traceback.format_exc()
            logger(measurement,error,s)



if __name__ == "__main__":
    subscribe_tickers(measurement)
    while True:
        time.sleep(60)
        subscribe_tickers(measurement)


