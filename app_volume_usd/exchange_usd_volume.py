import os
import sys
import time
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import numpy as np
current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.dirname(current_dir))

import api_coinbase.coinbaseRestApi as coinbase 
import api_kraken.KrakenRestApi as kraken
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
pd.set_option('display.float_format', lambda x: '%.3f' % x)

host_2 = InfluxClientHost2()
measurement = "log_usd_volume_report"
def write_data(measurement,data,exchange_tag):
    for symb in data:
        fields = {}
        tags = {}
        fields.update({"volume":data[symb]})
        tags.update({"symbol":symb})
        tags.update({"exchange":exchange_tag})
        dbtime = False
        host_2.write_points_to_measurement(measurement, dbtime, tags, fields)


ticker_cb = [t['id'] for t in coinbase.get_tickers() if t['quote_currency'] == "USD"]
data_cb = {}
data_delta_cb = {}
data_delta_percentage_cb = {}
vol_cb = 0 
for t in ticker_cb:
    time.sleep(1)
    data = coinbase.get_market_stats(t)
    data_prev = host_2.query_tables(measurement, ["*","where exchange = 'coinbase' and symbol = '{}' and time > now() - 1h order by time desc limit 1".format(t)],"raw")[0]['volume']
    volume = float(data['volume'])*float(data['last'])
    data_delta_cb.update({t:volume-data_prev})
    data_delta_percentage_cb.update({t:str((volume-data_prev)/data_prev*100)+"%"})
    vol_cb += volume
    data_cb.update({t:volume})
write_data(measurement, data_cb, "coinbase")
tickers_kr = [kraken.get_asset_pairs_info()[i] for i in kraken.get_asset_pairs_info()] 
#%%
ticker_kr = [i['altname'] for i in tickers_kr if i['quote'] == "ZUSD" and i['altname']!='ETHUSD.d' and i['altname']!='XBTUSD.d' and i['altname']!='GBPUSD' and i['altname'] != 'EURUSD']
data_kr = {}
data_delta_kr = {}
data_delta_percentage_kr = {}
vol_kr = 0 
for t in ticker_kr:
    time.sleep(0.001)
    data = [kraken.get_tickers(t)[i] for i in kraken.get_tickers(t)]
    data_prev = host_2.query_tables(measurement, ["*","where exchange = 'kraken' and symbol = '{}' and time > now() - 1h order by time desc limit 1".format(t)],"raw")[0]['volume']
    data_delta_kr.update({t:volume-data_prev})
    data_delta_percentage_kr.update({t:str((volume-data_prev)/data_prev*100)+"%"})
    volume = float(data[0]['v'][1]) * float(data[0]['c'][0])
    vol_kr += volume
    data_kr.update({t:volume})
write_data(measurement, data_kr, "kraken")
#%%
vol_total = vol_cb + vol_kr
data_total = {"vol_total":vol_total,"vol_cb":vol_cb,"vol_kr":vol_kr}
write_data(measurement, data_total, "agg")
df_coinabse = pd.DataFrame([data_cb,data_delta_cb,data_delta_percentage_cb],index=['volume','volume_change_hourly','volume_change_percentage']).T
df_kraken = pd.DataFrame([data_kr,data_delta_kr,data_delta_percentage_kr],index=['volume','volume_change_hourly','volume_change_percentage']).T
print(df_coinbase.to_string())
print(df_kraken.to_string())