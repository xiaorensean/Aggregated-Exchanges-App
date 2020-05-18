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


def usd_volume_report():
    ticker_cb = [t['id'] for t in coinbase.get_tickers() if t['quote_currency'] == "USD"]
    data_cb = {}
    data_delta_cb = {}
    data_delta_percentage_cb = {}
    vol_cb = 0 
    for t in ticker_cb:
        print(t)
        time.sleep(1)
        data = coinbase.get_market_stats(t)
        try:
            data_prev = host_2.query_tables(measurement, ["*","where exchange = 'coinbase' and symbol = '{}' order by time desc limit 1".format(t)],"raw")[0]['volume']
        except IndexError:
            data_new = {t:float(data['volume'])*float(data['last'])}
            write_data(measurement, data_new, "coinbase")
        volume = float(data['volume'])*float(data['last'])
        data_delta_cb.update({t:volume-data_prev})
        data_delta_percentage_cb.update({t:str((volume-data_prev)/data_prev*100)+"%"})
        vol_cb += volume
        data_cb.update({t:volume})
    write_data(measurement, data_cb, "coinbase")
    tickers_kr = [kraken.get_asset_pairs_info()[i] for i in kraken.get_asset_pairs_info()] 
    
    # Coinbase
    ticker_kr = [i['altname'] for i in tickers_kr if i['quote'] == "ZUSD" and i['altname']!='ETHUSD.d' and i['altname']!='XBTUSD.d' and i['altname']!='GBPUSD' and i['altname'] != 'EURUSD']
    data_kr = {}
    data_delta_kr = {}
    data_delta_percentage_kr = {}
    vol_kr = 0 
    for t in ticker_kr:
        time.sleep(0.001)
        data = [kraken.get_tickers(t)[i] for i in kraken.get_tickers(t)]
        data_prev = host_2.query_tables(measurement, ["*","where exchange = 'kraken' and symbol = '{}' order by time desc limit 1".format(t)],"raw")[0]['volume']
        volume = float(data[0]['v'][1]) * float(data[0]['c'][0])
        data_delta_kr.update({t:volume-data_prev})
        data_delta_percentage_kr.update({t:str((volume-data_prev)/data_prev*100)+"%"})
        vol_kr += volume
        data_kr.update({t:volume})
    write_data(measurement, data_kr, "kraken")
    # Kraken
    vol_total = vol_cb + vol_kr
    data_total = {"vol_total":vol_total,"vol_cb":vol_cb,"vol_kr":vol_kr}
    vol_total_prev = host_2.query_tables(measurement, ["*","where exchange = 'agg' and symbol = 'vol_total' order by time desc limit 1".format(t)],"raw")[0]['volume']
    vol_cb_prev = host_2.query_tables(measurement, ["*","where exchange = 'agg' and symbol = 'vol_cb' order by time desc limit 1".format(t)],"raw")[0]['volume']
    vol_kr_prev = host_2.query_tables(measurement, ["*","where exchange = 'agg' and symbol = 'vol_kr' order by time desc limit 1".format(t)],"raw")[0]['volume']
    write_data(measurement, data_total, "agg")
    vol_total_delta = vol_total - vol_total_prev
    vol_cb_delta = vol_cb - vol_cb_prev
    vol_kr_delta = vol_kr - vol_kr_prev
    df_coinbase = pd.DataFrame([data_cb,data_delta_cb,data_delta_percentage_cb],index=['volume','volume_change_hourly','volume_change_percentage']).T
    df_kraken = pd.DataFrame([data_kr,data_delta_kr,data_delta_percentage_kr],index=['volume','volume_change_hourly','volume_change_percentage']).T
    cb_report = df_coinbase.to_html()
    kr_report = df_kraken.to_html()

    # send email with tables
    msg = MIMEMultipart()
    msg['Subject'] = "24H USD Volume Report Hourly"
    msg['From'] = 'xiao@virgilqr.com'

    html = """\
    <html>
      <head></head>
      <body>
      <h1 style="font-size:15px;"> Total USD Volume Summary: </h1>
      <p> Total Current 24H USD Volume: {} </p>
      <p> Total 24H USD Volume Change: {} </p>
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
          """.format(vol_total,vol_total_delta,vol_cb, vol_cb_delta,cb_report,vol_kr, vol_kr_delta,kr_report,)

    part1 = MIMEText(html, 'html')
    msg.attach(part1) 
    smtp = smtplib.SMTP('smtp.gmail.com',587)
    smtp.starttls()
    smtp.login("vpfa.reports@gmail.com","921211@Rx")
    #smtp.sendmail("report",["vpfa.reports@gmail.com","nasir@virgilqr.com"], msg.as_string())
    smtp.sendmail("report",["vpfa.reports@gmail.com"], msg.as_string())
    smtp.quit()
    
    
    
if __name__ == "__main__":
    usd_volume_report()
    while True:
        time.sleep(60*60)
        usd_volume_report()
    
