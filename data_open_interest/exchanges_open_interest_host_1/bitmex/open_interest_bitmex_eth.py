import time
import websocket
import datetime
import json
import copy
import pandas as pd
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

def write_instrument_data(data):
    measurement = "bitmex_instrument_v1"
    for d in data:
        dbtime = False
        tags = {}
        tags.update({'symbol':d['symbol']})
        fields = copy.copy(d)
        del fields['symbol']
        for key, value in fields.items():
            if type(value) == int:
                fields[key] = float(value)
        host_1.write_points_to_measurement(measurement, dbtime, tags, fields) 
    
    
def subscribe_instrument_symbol(suffix = 'instrument,funding,insurance,settlement,liquidation,trade,chat'):
	connect_string = 'wss://www.bitmex.com/realtime?subscribe=' + suffix
	ws = websocket.WebSocketApp(connect_string,
		on_message = on_message_symbol,
        on_error = on_error,
        on_close = on_close)
	ws.run_forever(ping_interval=60, ping_timeout=10)
	
def on_message_symbol(ws, message):
    current_time = str(datetime.datetime.now())
    response = None
    try:
        response = json.loads(message)
    except:
        return
    endpoint = None
    data = None
    action = None
    try:
        endpoint = response["table"]
        data = response["data"]
        if len(data) == 1:
            if data[0]['symbol'] == 'ETHUSD':
                write_instrument_data(data)
                #df = pd.DataFrame(data)
                #df.to_excel(current_dir+"/data/" + current_time + ".xlsx")
                print(data)
            else:
                pass
        elif len(data) > 1:
            for d in data:
                if d['symbol'] == 'ETHUSD':
                    data = [d]
                    write_instrument_data(data)
                    #df = pd.DataFrame(data)
                    #df.to_excel(current_dir+"/data/" + current_time + ".xlsx")
                    print(d)
                else:
                    pass
        else:
            pass
        #df = pd.DataFrame(data)
        #df.to_excel(current_dir+"/data/" + current_time + ".xlsx")
        #print(data)
        action = response["action"]
    except:
        pass

def on_message(ws, message):
    current_time = str(datetime.datetime.now())
    response = None
    try:
        response = json.loads(message)
    except:
        return
    endpoint = None
    data = None
    action = None
    try:
        endpoint = response["table"]
        data = response["data"]
        if len(data) == 1:
            if data[0]['symbol'] == 'XBTUSD':
                df = pd.DataFrame(data)
                df.to_excel(current_dir+"/data/" + current_time + ".xlsx")
                print(data)
            else:
                pass
        elif len(data) > 1:
            for d in data:
                if d['symbol'] == 'XBTUSD':
                    df = pd.DataFrame(data)
                    df.to_excel(current_dir+"/data/" + current_time + ".xlsx")
                    print(data)
                else:
                    pass
        else:
            pass
        #df = pd.DataFrame(data)
        #df.to_excel(current_dir+"/data/" + current_time + ".xlsx")
        #print(data)
        action = response["action"]
    except:
        pass


def on_error(ws, error):
	sys.exit(1)


def on_close(ws):
    sys.exit(1)



if __name__ == "__main__":
    subscribe_instrument_symbol("instrument")
