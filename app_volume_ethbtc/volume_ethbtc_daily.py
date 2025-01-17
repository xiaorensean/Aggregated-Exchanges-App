import os
import sys
import time
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import numpy as np
current_dir = os.path.dirname(os.path.abspath(__file__))
pd.set_option('display.float_format', lambda x: '%.2f' % x)
sys.path.append(os.path.dirname(current_dir))

from api_binance.BinanceRestApi import get_spot24
from api_coinbase.coinbaseRestApi import get_market_stats
from api_huobi.HuobiRestApi import get_spot_market_info
from api_okex.OkexRestApi import get_spot_tickers
from api_kraken.KrakenRestApi import get_tickers
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
pd.set_option('display.float_format', lambda x: '%.3f' % x)

db = InfluxClientHost2()
measurement = "log_ethbtc_volume_report"

def checkIfUTCMidnight():
    utcnow = datetime.datetime.utcnow()
    seconds_since_utcmidnight = (utcnow - utcnow.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    if int(seconds_since_utcmidnight) == 0:
        run_script = True
        time.sleep(1)
    else:
        run_script = False
    return run_script

def get_vol_7d(symbol, exchange):
    db = InfluxClientHost2()
    data = db.query_tables(measurement, ["*", "where time > now() - 7d and exchange = '{}'".format(exchange)], "raw")
    vol_tot = 0
    numPeriod = 0
    for db in data:
        numPeriod += 1
        vol_tot += db[symbol]
    vol_7d = vol_tot / numPeriod
    return vol_7d

def get_vol_30d(symbol, exchange):
    db = InfluxClientHost2()
    data = db.query_tables(measurement, ["*", "where time > now() - 30d and exchange = '{}'".format(exchange)], "raw")
    vol_tot = 0
    numPeriod = 0
    for db in data:
        numPeriod += 1
        vol_tot += db[symbol]
    vol_7d = vol_tot / numPeriod
    return vol_7d

def get_vol_90d(symbol, exchange):
    db = InfluxClientHost2()
    data = db.query_tables(measurement, ["*", "where time > now() - 90d and exchange = '{}'".format(exchange)], "raw")
    vol_tot = 0
    numPeriod = 0
    for db in data:
        numPeriod += 1
        vol_tot += db[symbol]
    vol_7d = vol_tot / numPeriod
    return vol_7d


def value_type_convert(value):
    v1 = float("{:.2f}".format(round(value, 2)))
    return format(v1, ",")


def write_log(exchange, btc_volume, eth_volume):
    measurement = "log_ethbtc_volume_report"
    fields = {}
    fields.update({"btc_volume": btc_volume})
    fields.update({"eth_volume": eth_volume})
    tags = {}
    tags.update({"exchange": exchange})
    tags.update({"symbol": "ETHBTC"})
    dbtime = False
    db.write_points_to_measurement(measurement, dbtime, tags, fields)


def data_df(exchange, btc_volume, eth_volume):
    dbb = db.query_tables(measurement, ["*",
                                        "where exchange = '{}' and symbol = 'ETHBTC' and time > now() - 1d order by time limit 1".format(exchange)])
    btc_volume_delta = btc_volume - dbb['btc_volume'].tolist()[0]
    eth_volume_delta = eth_volume - dbb['eth_volume'].tolist()[0]
    btc_volume_per = str(
        np.round((btc_volume - dbb['btc_volume'].tolist()[0]) / dbb['btc_volume'].tolist()[0] * 100, decimals=2)) + "%"
    eth_volume_per = str(
        np.round((eth_volume - dbb['eth_volume'].tolist()[0]) / dbb['eth_volume'].tolist()[0] * 100, decimals=2)) + "%"
    btc_vol_7d = get_vol_7d("btc_volume", exchange)
    eth_vol_7d = get_vol_7d("eth_volume", exchange)
    dfb = pd.DataFrame([exchange, value_type_convert(btc_volume), value_type_convert(btc_volume_delta), btc_volume_per,
                        value_type_convert(btc_vol_7d), value_type_convert(eth_volume),
                        value_type_convert(eth_volume_delta), eth_volume_per, value_type_convert(eth_vol_7d)])
    dfb = dfb.T
    return dfb


def get_relative_percentage(symbol,exchange):
    d7 = get_vol_7d(symbol,exchange)
    d30 = get_vol_30d(symbol,exchange)
    d90 = get_vol_90d(symbol,exchange)
    delta30 = d30 - d7
    delta30_per = (d30 - d7)/d7*100
    delta90 = d90 - d7
    delta90_per = (d90 - d7)/d7*100
    df = pd.DataFrame([str(np.round(delta30_per,3))+"%",str(np.round(delta90_per,3))+"%"],columns=[exchange])
    #df.index = ["30VS7","90VS7"]
    return df

def volume_report():
    # binance
    # quote volume is in BTC
    symbol_binance = "ETHBTC"
    data_binance = get_spot24(symbol_binance)
    exchange_b = "Binance"
    btc_volume_b = float(data_binance['quoteVolume'])
    eth_volume_b = float(data_binance['volume'])
    write_log(exchange_b, btc_volume_b, eth_volume_b)
    dfb = data_df(exchange_b, btc_volume_b, eth_volume_b)


    # coinbase
    #  volume is in base currency units
    symbol_coinbase = "ETH-BTC"
    data_coinbase = get_market_stats(symbol_coinbase)
    exchange_c = "Coinbase"
    eth_volume_c = float(data_coinbase['volume'])
    btc_volume_c = float(data_coinbase['last']) * eth_volume_c
    write_log(exchange_c, btc_volume_c, eth_volume_c)
    dfc = data_df(exchange_c, btc_volume_c, eth_volume_c)


    # Huobi
    # volume is in base currency units
    symbol_huobi = "ethbtc"
    data_huobi = get_spot_market_info(symbol_huobi)['tick']
    exchange_h = "Huobi"
    btc_volume_h = float(data_huobi['vol'])
    eth_volume_h = btc_volume_h / float(data_huobi['close'])
    write_log(exchange_h, btc_volume_h, eth_volume_h)
    dfh = data_df(exchange_h, btc_volume_h, eth_volume_h)


    # Okex
    # volume is in eth
    symbol_okex = "eth_btc"
    data_okex = get_spot_tickers(symbol_okex)['data']
    exchange_o = "Okex"
    btc_volume_o = float(data_okex[0]['coinVolume'])
    eth_volume_o = float(data_okex[0]['volume'])
    write_log(exchange_o, btc_volume_o, eth_volume_o)
    dfo = data_df(exchange_o, btc_volume_o, eth_volume_o)


    # kraken
    # volume in BTC
    symbol_kraken = "XETHXXBT"
    data_kraken = get_tickers(symbol_kraken)["XETHXXBT"]
    exchange_k = "Kraken"
    # current volume
    # btc_volume_k = float(data_kraken['v'][0])
    # 24h volume
    eth_volume_k = float(data_kraken['v'][1])
    btc_volume_k = eth_volume_k * float(data_kraken['c'][0])
    write_log(exchange_k, btc_volume_k, eth_volume_k)
    dfk = data_df(exchange_k, btc_volume_k, eth_volume_k)


    df = pd.concat([dfb, dfc, dfh, dfo, dfk])
    df.columns = ["Exchange", "BTC_volume", "BTC_volume_change", "BTC_volume_percentage", "BTC_volume_7d", "ETH_volume",
                  "ETH_volume_change", "ETH_volume_percentage", "ETH_volume_7d"]
    report = df.to_html(index=False)

    # relative changes
    dfs = []
    exchanges = ["Binance", "Coinbase", "Huobi", "Okex", "Kraken"]
    for e in exchanges:
        dfs.append(get_relative_percentage("btc_volume", e))
    all_exchange = pd.concat(dfs, axis=1)
    all_exchange.index = ["30d_VS_7d", "90d_VS_7d"]
    report_relative_per = all_exchange.to_html()

    # gmail part
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "ETHBTC 24H Volume Report Daily"
    msg['From'] = 'xiao@virgilqr.com'

    html = """\
    <html>
      <head></head>
      <body>
      <h1 style="font-size:15px;"> Volume Report: </h1>
          {}
      <h2 style="font-size:15px;"> Relative Change Percentage: </h2>
          {}
      </body>
    </html>
          """.format(report,report_relative_per)

    part1 = MIMEText(html, 'html')
    msg.attach(part1)

    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    # smtp = smtplib.SMTP_SSL('smtp.gmail.com')
    # smtp.set_debuglevel(1)
    smtp.starttls()
    smtp.login("vpfa.reports@gmail.com", "921211@Rx")
    smtp.sendmail("report",["vpfa.reports@gmail.com","nasir@virgilqr.com"], msg.as_string())
    #smtp.sendmail("report", ["vpfa.reports@gmail.com"], msg.as_string())
    smtp.quit()


if __name__ == "__main__":
    #volume_report()
    while True:
        #time.sleep(60*60*24)
        if checkIfUTCMidnight():
            try:
                volume_report()
            except:
                time.sleep(60*60)
                pass
