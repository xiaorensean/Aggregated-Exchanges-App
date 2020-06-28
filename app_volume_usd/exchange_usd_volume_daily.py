import os
import sys
import time
import datetime
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
from api_bitfinex.BfxRest import BITFINEXCLIENT
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
pd.set_option('display.float_format', lambda x: '%.2f' % x)


bapi = BITFINEXCLIENT("","")

host_2 = InfluxClientHost2()
measurement = "log_usd_volume_report"

def checkIfUTCMidnight():
    utcnow = datetime.datetime.utcnow()
    seconds_since_utcmidnight = (utcnow - utcnow.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    if int(seconds_since_utcmidnight) == 0:
        run_script = True
        time.sleep(1)
    else:
        run_script = False
    return run_script

def value_type_convert(value):
    v1 = float("{:.2f}".format(round(value, 2)))
    return format(v1,",")

def write_data(measurement,data,exchange_tag):
    for symb in data:
        fields = {}
        tags = {}
        fields.update({"volume":data[symb]})
        tags.update({"symbol":symb})
        tags.update({"exchange":exchange_tag})
        dbtime = False
        host_2.write_points_to_measurement(measurement, dbtime, tags, fields)

def usd_volume_report():
    # bitfinex
    data_bf = {}
    data_bf_db = {}
    data_bf_delta = {}
    data_delta_percentage_bf = {}
    vol_bf = 0
    data = bapi.get_public_tickers("ALL")
    for d in data:
        if "USD" in d[0] and d[0] != "fUSD":
            print(d[0], float(d[8]) * float(d[7]))
            data_prev = host_2.query_tables(measurement, ["*","where exchange = 'bitfinex' and symbol = '{}' and time >= now() - 1d order by time limit 1".format(d[0])], "raw")[0]['volume']
            volume = float(d[8]) * float(d[7])
            data_bf_delta.update({d[0]:value_type_convert(volume-data_prev)})
            try:
                data_delta_percentage_bf.update({d[0]: value_type_convert((volume - data_prev) / data_prev * 100) + "%"})
            except:
                data_delta_percentage_bf.update({d[0]: str(0.00) + "%"})
            vol_bf += volume
            data_bf.update({d[0]: value_type_convert(volume)})
            data_bf_db.update({d[0]: volume})
        else:
            pass
    write_data(measurement, data_bf_db, "bitfinex")
    # coinbase
    ticker_cb = [t['id'] for t in coinbase.get_tickers() if t['quote_currency'] == "USD"]
    data_cb = {}
    data_cb_db = {}
    data_delta_cb = {}
    data_delta_percentage_cb = {}
    vol_cb = 0
    for t in ticker_cb:
        print(t)
        time.sleep(2)
        data = coinbase.get_market_stats(t)
        try:
            data_prev = host_2.query_tables(measurement, ["*","where exchange = 'coinbase' and symbol = '{}' and time >= now() - 1d order by time limit 1".format(t)], "raw")[0]['volume']
        except IndexError:
            data_new = {t: float(data['volume']) * float(data['last'])}
            write_data(measurement, data_new, "coinbase")
        volume = float(data['volume']) * float(data['last'])
        data_delta_cb.update({t: value_type_convert(volume - data_prev)})
        try:
            data_delta_percentage_cb.update({t: value_type_convert((volume - data_prev) / data_prev * 100) + "%"})
        except:
            data_delta_percentage_cb.update({t: str(0.00) + "%"})
        vol_cb += volume
        data_cb.update({t: value_type_convert(volume)})
        data_cb_db.update({t: volume})
    write_data(measurement, data_cb_db, "coinbase")

    # Kraken
    tickers_kr = [kraken.get_asset_pairs_info()[i] for i in kraken.get_asset_pairs_info()]
    ticker_kr = [i['altname'] for i in tickers_kr if
                 i['quote'] == "ZUSD" and i['altname'] != 'ETHUSD.d' and i['altname'] != 'XBTUSD.d' and i[
                     'altname'] != 'GBPUSD' and i['altname'] != 'EURUSD']
    data_kr = {}
    data_kr_db = {}
    data_delta_kr = {}
    data_delta_percentage_kr = {}
    vol_kr = 0
    for t in ticker_kr:
        time.sleep(0.001)
        data = [kraken.get_tickers(t)[i] for i in kraken.get_tickers(t)]
        try:
            data_prev = host_2.query_tables(measurement, ["*","where exchange = 'kraken' and symbol = '{}' and time >= now() - 1d order by time limit 1".format(t)], "raw")[0]['volume']
        except IndexError:
            data_new = {t: float(data['volume']) * float(data['last'])}
            write_data(measurement, data_new, "kraken")
        volume = float(data[0]['v'][1]) * float(data[0]['c'][0])
        data_delta_kr.update({t: value_type_convert(volume - data_prev)})
        try:
            data_delta_percentage_kr.update({t: value_type_convert((volume - data_prev) / data_prev * 100) + "%"})
        except:
            data_delta_percentage_kr.update({t: str(0.00) + "%"})
        vol_kr += volume
        data_kr.update({t: value_type_convert(volume)})
        data_kr_db.update({t: volume})
    write_data(measurement, data_kr_db, "kraken")
    # Aggregate
    vol_total = vol_cb + vol_kr + vol_bf
    data_total = {"vol_total": vol_total, "vol_cb": vol_cb, "vol_kr": vol_kr, "vol_bf":vol_bf}
    write_data(measurement, data_total, "agg")
    vol_total_prev = host_2.query_tables(measurement, ["*","where exchange = 'agg' and symbol = 'vol_total' and time >= now() - 1d order by time limit 1"], "raw")[0]['volume']
    vol_bf_prev = host_2.query_tables(measurement, ["*","where exchange = 'agg' and symbol = 'vol_bf' and time >= now() - 1d order by time limit 1"], "raw")[0]['volume']
    vol_cb_prev = host_2.query_tables(measurement, ["*","where exchange = 'agg' and symbol = 'vol_cb' and time >= now() - 1d order by time limit 1"], "raw")[0]['volume']
    vol_kr_prev = host_2.query_tables(measurement, ["*","where exchange = 'agg' and symbol = 'vol_kr' and time >= now() - 1d order by time limit 1"], "raw")[0]['volume']
    vol_total_delta = vol_total - vol_total_prev
    vol_bf_delta = vol_bf - vol_bf_prev
    vol_cb_delta = vol_cb - vol_cb_prev
    vol_kr_delta = vol_kr - vol_kr_prev
    df_bitfinex = pd.DataFrame([data_bf, data_bf_delta, data_delta_percentage_bf],
                               index=['volume', 'volume_change_daily', 'volume_change_percentage']).T
    df_coinbase = pd.DataFrame([data_cb, data_delta_cb, data_delta_percentage_cb],
                               index=['volume', 'volume_change_daily', 'volume_change_percentage']).T
    df_kraken = pd.DataFrame([data_kr, data_delta_kr, data_delta_percentage_kr],
                             index=['volume', 'volume_change_daily', 'volume_change_percentage']).T
    bf_report = df_bitfinex.to_html()
    cb_report = df_coinbase.to_html()
    kr_report = df_kraken.to_html()

    # send email with tables
    msg = MIMEMultipart()
    msg['Subject'] = "24H USD Volume Report Daily"
    msg['From'] = 'xiao@virgilqr.com'

    html = """\
     <html>
       <head></head>
       <body>
       <h1 style="font-size:15px;"> Total USD Volume Summary: </h1>
       <p> Total Current 24H USD Volume: {} </p>
       <p> Total 24H USD Volume Change: {} </p>
       <h1 style="font-size:15px;"> Bitfinex USD Volume Summary: </h1>
       <p> Bitfinex Current 24H USD Volume: {} </p>
       <p> Bitfinex 24H USD Volume Change: {} </p>
       <h3 style="font-size:15px;"> Bitfinex Tickers Breakdown: </h3>
       <p>
            {}
       </p>
       <h1 style="font-size:15px;"> Coinbase USD Volume Summary: </h1>
       <p> Coinbase Current 24H USD Volume: {} </p>
       <p> Coinbase 24H USD Volume Change: {} </p>
       <h3 style="font-size:15px;"> Coinbase Tickers Breakdown: </h3>
       <p>
            {}
       </p>
       <h1 style="font-size:15px;"> Kraken USD Volume Summary: </h1>
       <p> Kraken Current 24H USD Volume: {} </p>
       <p> Kraken 24H USD Volume Change: {} </p>
       <h3 style="font-size:15px;"> Kraken TIckers Breakdown: </h3>
       <p>
            {}
       </p>
       </body>
     </html>
           """.format(value_type_convert(vol_total), value_type_convert(vol_total_delta),
                      value_type_convert(vol_bf), value_type_convert(vol_bf_delta), bf_report,
                      value_type_convert(vol_cb),value_type_convert(vol_cb_delta), cb_report,
                      value_type_convert(vol_kr),value_type_convert(vol_kr_delta), kr_report, )

    part1 = MIMEText(html, 'html')
    msg.attach(part1)
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.starttls()
    smtp.login("vpfa.reports@gmail.com", "921211@Rx")
    smtp.sendmail("report",["vpfa.reports@gmail.com","nasir@virgilqr.com"], msg.as_string())
    #smtp.sendmail("report", ["vpfa.reports@gmail.com"], msg.as_string())
    smtp.quit()
    
    
if __name__ == "__main__":
    #usd_volume_report()
    while True:
        #time.sleep(60*60*24)
        if checkIfUTCMidnight():
            try:
                usd_volume_report()
            except:
                time.sleep(60*60)
                usd_volume_report()
    
