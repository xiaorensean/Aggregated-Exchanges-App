import pandas as pd
import websocket
import zlib
import json
from threading import Timer
from termcolor import colored
import time
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

url = 'wss://real.okex.com:10442/ws/v3'

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

def timeout_ping(ws):
    ws.send("ping")

def subscribe_spot_tikcer(endpoint):
    ws = websocket.create_connection(url)
    sub_param = {"op": "subscribe", "args": endpoint}
    sub_str = json.dumps(sub_param)
    ws.send(sub_str)
    while (True):
        t = Timer(5, timeout_ping, args=[ws,])
        t.start()
        res = ws.recv()
        t.cancel()
        res = inflate(res)
        response = None
        try:
            response = json.loads(res)
        except:
            continue
        try:
            response_type = response["table"]
        except:
            #Initial response to confirm subscription
            try:
                response_type = response["event"]
                continue
            #Some kind of error
            except:
                print(colored("Invalid response from server", "red"))
                ws.close()
                sys.exit(0)
        data = response["data"]
        if len(data) == 1:
            volume = {data[0]['instrument_id']:float(data[0]['quote_volume_24h'])}   
        else:
            volume = None
        print(volume)
        time.sleep(3)


def get_spot_ticker(endpoint):
    ws = websocket.create_connection(url)
    sub_param = {"op": "subscribe", "args": endpoint}
    sub_str = json.dumps(sub_param)
    ws.send(sub_str)
    res = ws.recv()
    res = inflate(res)
    # initial response
    response = json.loads(res)
    # get table
    res = ws.recv()
    res = inflate(res)
    response = json.loads(res)
    data = response["data"]
    ws.close()
    return data[0]





def volume_report_okex(endpoint):
    all_data = []
    related_symbol = str()
    for ep in endpoint:
        all_data.append(get_spot_ticker(ep))  
        if 'BSV' in ep:
            related_symbol = 'bsv'
        elif 'BCH' in ep:
            related_symbol = 'bch'
    volume_base = {}
    volume_quote = {}
    volume_base_total = 0
    volume_quote_total = 0
    for ad in all_data:
        volume_base.update({ad['instrument_id']:float(ad['base_volume_24h'])})
        volume_quote.update({ad['instrument_id']:float(ad['quote_volume_24h'])})
        volume_base_total += float(ad['base_volume_24h'])
        volume_quote_total += float(ad['quote_volume_24h'])
    volume_base.update({"total_okex":volume_base_total})
    volume_quote.update({"total_okex_"+related_symbol:volume_quote_total})
    return volume_quote

if __name__ == "__main__":
    bsv = ["spot/ticker:BSV-USDT","spot/ticker:BSV-USDK"]
    bch = ["spot/ticker:BCH-USDT","spot/ticker:BCH-USDK"]
    bsv_report = volume_report_okex(bsv)
    bch_report = volume_report_okex(bch)



