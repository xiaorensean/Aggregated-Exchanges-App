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
from api_binance.BinanceRestApi import get_spot24
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

def checkIfMorning():
    utcnow = datetime.datetime.utcnow()
    seconds_since_morning = (utcnow - utcnow.replace(hour=11, minute=0, second=0, microsecond=0)).total_seconds()
    if int(seconds_since_morning) == 0:
        run_script = True
        time.sleep(1)
    else:
        run_script = False
    return run_script

def get_vol_7d(exchange,symbol):
    data = host_2.query_tables(measurement, ["*", "where exchange = '{}' and symbol = '{}' and time > now() - 7d".format(exchange,symbol)], "raw")
    vol_tot = 0
    numPeriod = 0
    for db in data:
        numPeriod += 1
        vol_tot += float(db["volume"])
    vol_7d = vol_tot / numPeriod
    print(vol_tot)
    print(numPeriod)
    return vol_7d

def get_vol_30d(exchange,symbol):
    data = host_2.query_tables(measurement, ["*", "where exchange = '{}' and symbol = '{}' and time > now() - 30d".format(exchange,symbol)], "raw")
    vol_tot = 0
    numPeriod = 0
    for db in data:
        numPeriod += 1
        vol_tot += float(db["volume"])
    vol_30d = vol_tot / numPeriod
    print(vol_tot)
    print(numPeriod)
    return vol_30d

def get_vol_90d(exchange,symbol):
    data = host_2.query_tables(measurement, ["*", "where exchange = '{}' and symbol = '{}' and time > now() - 90d".format(exchange,symbol)], "raw")
    vol_tot = 0
    numPeriod = 0
    for db in data:
        numPeriod += 1
        vol_tot += float(db["volume"])
    vol_90d = vol_tot / numPeriod
    print(vol_tot)
    print(numPeriod)
    return vol_90d

def get_relative_percentage(exchange,symbol):
    d7 = get_vol_7d(exchange,symbol)
    d30 = get_vol_30d(exchange,symbol)
    d90 = get_vol_90d(exchange,symbol)
    delta30 = d30 - d7
    delta30_per = (d7 - d30)/d30*100
    delta90 = d90 - d7
    delta90_per = (d7 - d90)/d90*100
    df = pd.DataFrame([str(np.round(delta30_per,3))+"%",str(np.round(delta90_per,3))+"%"],columns=[symbol])
    #df.index = ["30VS7","90VS7"]
    return df

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

def get_delta_percentage(exchange,symbol,vol_total):
    vol_total_delta_7d = vol_total - get_vol_7d(exchange,symbol)
    vol_total_delta_30d = vol_total - get_vol_30d(exchange,symbol)
    vol_total_delta_90d = vol_total - get_vol_90d(exchange,symbol)
    vol_total_delta_7d_per = (vol_total - get_vol_7d(exchange,symbol))/get_vol_7d(exchange,symbol)*100
    vol_total_delta_30d_per = (vol_total - get_vol_30d(exchange,symbol))/get_vol_30d(exchange,symbol)*100
    vol_total_delta_90d_per = (vol_total - get_vol_90d(exchange,symbol))/get_vol_90d(exchange,symbol)*100
    return [vol_total_delta_7d, vol_total_delta_7d_per, vol_total_delta_30d, vol_total_delta_30d_per, vol_total_delta_90d, vol_total_delta_90d_per]

def get_binance_report():
    # binance
    usdt_pairs = [i for i in get_spot24() if "USDT" in i["symbol"]]
    print(usdt_pairs)

def usd_volume_report():
    # bitfinex
    data_bf = {}
    data_bf_db = {}
    data_bf_delta = {}
    data_delta_percentage_bf = {}
    data_bf_delta_7d = {}
    data_bf_delta_30d = {}
    data_bf_delta_90d = {}
    data_bf_delta_7d_per = {}
    data_bf_delta_30d_per = {}
    data_bf_delta_90d_per = {}
    volume_bf = 0
    vol_bf = 0
    data = bapi.get_public_tickers("ALL")
    for d in data:
        if "USD" in d[0] and d[0] != "fUSD":
            print(d[0], float(d[8]) * float(d[7]))
            data_prev = host_2.query_tables(measurement, ["*","where exchange = 'bitfinex' and symbol = '{}' and time >= now() - 1d order by time limit 1".format(d[0])], "raw")[0]['volume']
            data_prev_7d = get_vol_7d("bitfinex",d[0])
            data_prev_30d = get_vol_30d("bitfinex",d[0])
            data_prev_90d = get_vol_90d("bitfinex", d[0])
            volume_bf = float(d[8]) * float(d[7])
            data_bf_delta.update({d[0]:value_type_convert(volume_bf-data_prev)})
            data_bf_delta_7d.update({d[0]:value_type_convert(volume_bf-data_prev_7d)})
            data_bf_delta_30d.update({d[0]: value_type_convert(volume_bf - data_prev_30d)})
            data_bf_delta_90d.update({d[0]: value_type_convert(volume_bf - data_prev_90d)})
            # Daily changes Percentage
            try:
                data_delta_percentage_bf.update({d[0]: value_type_convert((volume_bf - data_prev) / data_prev * 100) + "%"})
            except:
                data_delta_percentage_bf.update({d[0]: str(0.00) + "%"})
            # 7d changes Percentage
            try:
                data_bf_delta_7d_per.update({d[0]: value_type_convert((volume_bf - data_prev_7d) / data_prev_7d * 100) + "%"})
            except:
                data_bf_delta_7d_per.update({d[0]: str(0.00) + "%"})
            # 30d changes Percentage
            try:
                data_bf_delta_30d_per.update({d[0]: value_type_convert((volume_bf - data_prev_30d) / data_prev_30d * 100) + "%"})
            except:
                data_bf_delta_30d_per.update({d[0]: str(0.00) + "%"})
            # 90d changes Percentage
            try:
                data_bf_delta_90d_per.update({d[0]: value_type_convert((volume_bf - data_prev_90d) / data_prev_90d * 100) + "%"})
            except:
                data_bf_delta_90d_per.update({d[0]: str(0.00) + "%"})

            vol_bf += volume_bf
            data_bf.update({d[0]: value_type_convert(volume_bf)})
            data_bf_db.update({d[0]: volume_bf})
        else:
            pass
    write_data(measurement, data_bf_db, "bitfinex")

    # coinbase
    ticker_cb = [t['id'] for t in coinbase.get_tickers() if t['quote_currency'] == "USD"]
    data_cb = {}
    data_cb_db = {}
    data_delta_cb = {}
    data_delta_percentage_cb = {}
    data_cb_delta_7d = {}
    data_cb_delta_30d = {}
    data_cb_delta_90d = {}
    data_cb_delta_7d_per = {}
    data_cb_delta_30d_per = {}
    data_cb_delta_90d_per = {}
    vol_cb = 0
    for t in ticker_cb:
        print(t)
        time.sleep(2)
        data = coinbase.get_market_stats(t)
        try:
            data_prev_cb = host_2.query_tables(measurement, ["*","where exchange = 'coinbase' and symbol = '{}' and time >= now() - 1d order by time limit 1".format(t)], "raw")[0]['volume']
        except IndexError:
            data_prev_cb = float(data['volume']) * float(data['last'])
            fields = {}
            tags = {}
            fields.update({"volume": data_prev_cb})
            tags.update({"symbol": t})
            tags.update({"exchange": "coinbase"})
            dbtime = False
            host_2.write_points_to_measurement(measurement, dbtime, tags, fields)
        data_prev_7d_cb = get_vol_7d("coinbase",t)
        data_prev_30d_cb = get_vol_30d("coinbase",t)
        data_prev_90d_cb = get_vol_90d("coinbase",t)
        try:
            volume = float(data['volume']) * float(data['last'])
        except:
            volume = 0
        data_delta_cb.update({t: value_type_convert(volume - data_prev_cb)})
        data_cb_delta_7d.update({t:value_type_convert(volume - data_prev_7d_cb)})
        data_cb_delta_30d.update({t: value_type_convert(volume - data_prev_30d_cb)})
        data_cb_delta_90d.update({t: value_type_convert(volume - data_prev_90d_cb)})
        try:
            data_delta_percentage_cb.update({t: value_type_convert((volume - data_prev_cb) / data_prev_cb * 100) + "%"})
        except:
            data_delta_percentage_cb.update({t: str(0.00) + "%"})
        try:
            data_cb_delta_7d_per.update({t:value_type_convert((volume - data_prev_7d_cb) / data_prev_7d_cb * 100) + "%"})
        except:
            data_cb_delta_7d_per.update({t: str(0.00) + "%"})
        try:
            data_cb_delta_30d_per.update({t:value_type_convert((volume - data_prev_30d_cb) / data_prev_30d_cb * 100) + "%"})
        except:
            data_cb_delta_30d_per.update({t: str(0.00) + "%"})
        try:
            data_cb_delta_90d_per.update({t: value_type_convert((volume - data_prev_90d_cb) / data_prev_90d_cb * 100) + "%"})
        except:
            data_cb_delta_90d_per.update({t: str(0.00) + "%"})
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
    data_kr_delta_7d = {}
    data_kr_delta_30d = {}
    data_kr_delta_90d = {}
    data_kr_delta_7d_per = {}
    data_kr_delta_30d_per = {}
    data_kr_delta_90d_per = {}
    vol_kr = 0
    for t in ticker_kr:
        time.sleep(0.001)
        data = [kraken.get_tickers(t)[i] for i in kraken.get_tickers(t)]
        print(data)
        try:
            data_prev_kr = host_2.query_tables(measurement, ["*","where exchange = 'kraken' and symbol = '{}' and time >= now() - 1d order by time limit 1".format(t)], "raw")[0]['volume']
        except:
            data_prev_kr = float(data[0]["v"][1])
            fields = {}
            tags = {}
            fields.update({"volume": data_prev_kr})
            tags.update({"symbol": t})
            tags.update({"exchange": "kraken"})
            dbtime = False
            host_2.write_points_to_measurement(measurement, dbtime, tags, fields)

        data_prev_7d_kr = get_vol_7d("kraken",t)
        data_prev_30d_kr = get_vol_30d("kraken",t)
        data_prev_90d_kr = get_vol_90d("kraken",t)
        volume = float(data[0]["v"][0])* float(data[0]['c'][0])
        data_delta_kr.update({t: value_type_convert(volume - data_prev_kr)})
        data_kr_delta_7d.update({t:value_type_convert(volume - data_prev_7d_kr)})
        data_kr_delta_30d.update({t:value_type_convert(volume-data_prev_30d_kr)})
        data_kr_delta_90d.update({t:value_type_convert(volume-data_prev_90d_kr)})
        try:
            data_delta_percentage_kr.update({t: value_type_convert((volume - data_prev_kr) / data_prev_kr * 100) + "%"})
        except:
            data_delta_percentage_kr.update({t: str(0.00) + "%"})
        try:
            data_kr_delta_7d_per.update({t: value_type_convert((volume - data_prev_7d_kr) / data_prev_7d_kr * 100) + "%"})
        except:
            data_kr_delta_7d_per.update({t: str(0.00) + "%"})
        try:
            data_kr_delta_30d_per.update({t: value_type_convert((volume - data_prev_30d_kr) / data_prev_30d_kr * 100) + "%"})
        except:
            data_kr_delta_30d_per.update({t: str(0.00) + "%"})
        try:
            data_kr_delta_90d_per.update({t: value_type_convert((volume - data_prev_90d_kr) / data_prev_90d_kr * 100) + "%"})
        except:
            data_kr_delta_90d_per.update({t: str(0.00) + "%"})
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
    symbol = ["vol_bf", "vol_cb", "vol_kr"]
    df_pers = []
    for symb in symbol:
        df_per = get_relative_percentage("agg",symb)
        df_pers.append(df_per)
    relative_per_df = pd.concat(df_pers, axis=1)
    relative_per_df.index = ["7d_VS_30d", "7d_VS_90d"]
    # total deltas & percentage
    vol_total_delta = vol_total - vol_total_prev
    delta_total = get_delta_percentage("agg","vol_total",vol_total)
    # bitfinex delta & percentage
    vol_bf_delta = vol_bf - vol_bf_prev
    delta_bf = get_delta_percentage("agg","vol_bf",vol_bf)
    # coinbase delta & percentage
    vol_cb_delta = vol_cb - vol_cb_prev
    delta_cb = get_delta_percentage("agg","vol_cb",vol_cb)
    # kraken delta & percentage
    vol_kr_delta = vol_kr - vol_kr_prev
    delta_kr = get_delta_percentage("agg","vol_kr",vol_kr)

    df_bitfinex = pd.DataFrame([data_bf, data_bf_delta, data_delta_percentage_bf, data_bf_delta_7d,data_bf_delta_7d_per,
                                data_bf_delta_30d,data_bf_delta_30d_per,data_bf_delta_90d,data_bf_delta_90d_per],
                                index=['volume', '1D_diff', '1D%','7D_diff', '7D%','30D_diff', '30D%','90D_diff', '90D%']).T
    df_coinbase = pd.DataFrame([data_cb, data_delta_cb, data_delta_percentage_cb, data_cb_delta_7d,data_cb_delta_7d_per,
                                data_cb_delta_30d, data_cb_delta_30d_per, data_cb_delta_90d, data_cb_delta_90d_per],
                                index=['volume', '1D_diff', '1D%','7D_diff', '7D%','30D_diff', '30D%','90D_diff', '90D%']).T
    df_kraken = pd.DataFrame([data_kr, data_delta_kr, data_delta_percentage_kr,data_kr_delta_7d, data_kr_delta_7d_per,
                              data_kr_delta_30d, data_kr_delta_30d_per, data_kr_delta_90d, data_kr_delta_90d_per],
                             index=['volume', '1D_diff', '1D%','7D_diff', '7D%','30D_diff', '30D%','90D_diff', '90D%']).T
    bf_report = df_bitfinex.to_html()
    cb_report = df_coinbase.to_html()
    kr_report = df_kraken.to_html()


    relative_per_report = relative_per_df.to_html()
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
       <p> Total 7D Mean USD Volume Change: {} </p>
       <p> Total 30D Mean USD Volume Change: {} </p>
       <p> Total 90D Mean USD Volume Change: {} </p>
       <h4 style="font-size:15px;"> Relative Changes 7d VS 30d && 7d VS 90d </h4>
       <p>
        {}
       </p>   
       <h1 style="font-size:15px;"> Bitfinex USD Volume Summary: </h1>
       <p> Bitfinex Current 24H USD Volume: {} </p>
       <p> Bitfinex 24H USD Volume Change: {} </p>
       <p> Bitfinex 7D Mean USD Volume Change: {} </p>
       <p> Bitfinex 30D Mean USD Volume Change: {} </p>
       <p> Bitfinex 90D Mean USD Volume Change: {} </p>
       <h3 style="font-size:15px;"> Bitfinex Tickers Breakdown: </h3>
       <p>
            {}
       </p>
       <h1 style="font-size:15px;"> Coinbase USD Volume Summary: </h1>
       <p> Coinbase Current 24H USD Volume: {} </p>
       <p> Coinbase 24H USD Volume Change: {} </p>
       <p> Coinbase 7D Mean USD Volume Change: {} </p>
       <p> Coinbase 30D Mean USD Volume Change: {} </p>
       <p> Coinbase 90D Mean USD Volume Change: {} </p>
       <h3 style="font-size:15px;"> Coinbase Tickers Breakdown: </h3>
       <p>
            {}
       </p>
       <h1 style="font-size:15px;"> Kraken USD Volume Summary: </h1>
       <p> Kraken Current 24H USD Volume: {} </p>
       <p> Kraken 24H USD Volume Change: {} </p>
       <p> Kraken 7D Mean USD Volume Change: {} </p>
       <p> Kraken 30D Mean USD Volume Change: {} </p>
       <p> Kraken 90D Mean USD Volume Change: {} </p>
       <h3 style="font-size:15px;"> Kraken TIckers Breakdown: </h3>
       <p>
            {}
       </p>
       </body>
     </html>
           """.format(value_type_convert(vol_total), value_type_convert(vol_total_delta),
                      value_type_convert(delta_total[0])+" ("+ value_type_convert(delta_total[1])+"%)",
                      value_type_convert(delta_total[2]) + " (" + value_type_convert(delta_total[3]) + "%)",
                      value_type_convert(delta_total[4]) + " (" + value_type_convert(delta_total[5]) + "%)",
                      relative_per_report,
                      value_type_convert(vol_bf), value_type_convert(vol_bf_delta),
                      value_type_convert(delta_bf[0]) + " (" + value_type_convert(delta_bf[1]) + "%)",
                      value_type_convert(delta_bf[2]) + " (" + value_type_convert(delta_bf[3]) + "%)",
                      value_type_convert(delta_bf[4]) + " (" + value_type_convert(delta_bf[5]) + "%)",
                      bf_report,
                      value_type_convert(vol_cb),value_type_convert(vol_cb_delta),
                      value_type_convert(delta_cb[0]) + " (" + value_type_convert(delta_cb[1]) + "%)",
                      value_type_convert(delta_cb[2]) + " (" + value_type_convert(delta_cb[3]) + "%)",
                      value_type_convert(delta_cb[4]) + " (" + value_type_convert(delta_cb[5]) + "%)",
                      cb_report,
                      value_type_convert(vol_kr),value_type_convert(vol_kr_delta),
                      value_type_convert(delta_kr[0]) + " (" + value_type_convert(delta_kr[1]) + "%)",
                      value_type_convert(delta_kr[2]) + " (" + value_type_convert(delta_kr[3]) + "%)",
                      value_type_convert(delta_kr[4]) + " (" + value_type_convert(delta_kr[5]) + "%)",
                      kr_report,
                      )

    part1 = MIMEText(html, 'html')
    msg.attach(part1)
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.starttls()
    smtp.login("vpfa.reports@gmail.com", "921211@Rx")
    #smtp.sendmail("report",["vpfa.reports@gmail.com","nasir@virgilqr.com"], msg.as_string())
    smtp.sendmail("report", ["vpfa.reports@gmail.com"], msg.as_string())
    smtp.quit()

#get_binance_report()
#usd_volume_report()

'''''
if __name__ == "__main__":
    #usd_volume_report()
    while True:
        #time.sleep(60*60*24)
        if checkIfUTCMidnight() or checkIfMorning():
            try:
                usd_volume_report()
            except:
                time.sleep(60*60)
                usd_volume_report()
'''
