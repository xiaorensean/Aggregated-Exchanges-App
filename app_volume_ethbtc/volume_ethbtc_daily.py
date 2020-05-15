import os
import sys
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import numpy as np
current_dir = os.path.dirname(os.path.abspath(__file__))

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


def write_log(exchange,btc_volume,eth_volume):
    measurement = "log_ethbtc_volume_report"
    fields = {}
    fields.update({"btc_volume":btc_volume})
    fields.update({"eth_volume":eth_volume})
    tags = {}
    tags.update({"exchange":exchange})
    tags.update({"symbol":"ETHBTC"})
    dbtime = False
    db.write_points_to_measurement(measurement, dbtime, tags, fields)    


def data_df(exchange,btc_volume,eth_volume):
    dbb = db.query_tables(measurement, ["*","where exchange = '{}' and symbol = 'ETHBTC' and time > now() - 1d order by time limit 1".format(exchange)])
    btc_volume_delta = btc_volume - dbb['btc_volume'].tolist()[0]
    eth_volume_delta = eth_volume - dbb['eth_volume'].tolist()[0]
    btc_volume_per = str(np.round((btc_volume - dbb['btc_volume'].tolist()[0])/dbb['btc_volume'].tolist()[0] * 100,decimals=3))+"%"
    eth_volume_per = str(np.round((eth_volume - dbb['eth_volume'].tolist()[0])/dbb['eth_volume'].tolist()[0] * 100,decimals=3))+"%"
    dfb = pd.DataFrame([exchange,btc_volume,btc_volume_delta,btc_volume_per,eth_volume,eth_volume_delta,eth_volume_per])
    dfb = dfb.T
    return dfb

def volume_report():
# binance
# quote volume is in BTC
    symbol_binance = "ETHBTC"
    data_binance = get_spot24(symbol_binance)
    exchange_b = "Binance"
    btc_volume_b = float(data_binance['quoteVolume'])
    eth_volume_b = float(data_binance['volume'])
    dfb = data_df(exchange_b,btc_volume_b,eth_volume_b)
    write_log(exchange_b, btc_volume_b, eth_volume_b)


# coinbase
#  volume is in base currency units
    symbol_coinbase = "ETH-BTC"
    data_coinbase = get_market_stats(symbol_coinbase)
    exchange_c = "Coinbase"
    eth_volume_c = float(data_coinbase['volume'])
    btc_volume_c = float(data_coinbase['last']) * eth_volume_c
    dfc = data_df(exchange_c,btc_volume_c,eth_volume_c)
    write_log(exchange_c, btc_volume_c, eth_volume_c)


# Huobi
# volume is in base currency units
    symbol_huobi = "ethbtc"
    data_huobi = get_spot_market_info(symbol_huobi)['tick']
    exchange_h = "Huobi"
    btc_volume_h = float(data_huobi['vol'])
    eth_volume_h = btc_volume_h/float(data_huobi['close'])
    dfh = data_df(exchange_h,btc_volume_h,eth_volume_h)
    write_log(exchange_h, btc_volume_h, eth_volume_h)


#Okex
# volume is in eth 
    symbol_okex = "eth_btc"
    data_okex = get_spot_tickers(symbol_okex)['data']
    exchange_o = "Okex"
    btc_volume_o = float(data_okex[0]['coinVolume'])
    eth_volume_o = float(data_okex[0]['volume'])
    dfo = data_df(exchange_o,btc_volume_o,eth_volume_o)
    write_log(exchange_o, btc_volume_o, eth_volume_o)


# kraken
# volume in BTC
    symbol_kraken = "XETHXXBT"
    data_kraken = get_tickers(symbol_kraken)["XETHXXBT"]
    exchange_k = "Kraken"
    # current volume 
    #btc_volume_k = float(data_kraken['v'][0])
    # 24h volume
    eth_volume_k = float(data_kraken['v'][1])
    btc_volume_k = eth_volume_k*float(data_kraken['c'][0])
    dfk = data_df(exchange_k,btc_volume_k,eth_volume_k)
    write_log(exchange_k, btc_volume_k, eth_volume_k)


    df = pd.concat([dfb,dfc,dfh,dfo,dfk])
    df.columns = ["Exchange","BTC_volume","BTC_volume_change","BTC_volume_percentage","ETH_volume","ETH_volume_change","ETH_volume_percentage"]
    report = df.to_html(index=False)   
    
    # gmail part
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "ETHBTC 24H Volume Report Daily"
    msg['From'] = 'xiao@virgilqr.com'

    html = """\
    <html>
      <head></head>
      <body>
          {}
      </body>
    </html>
          """.format(report)

    part1 = MIMEText(html, 'html')
    msg.attach(part1)

    smtp = smtplib.SMTP('smtp.gmail.com',587)
    #smtp = smtplib.SMTP_SSL('smtp.gmail.com')
    #smtp.set_debuglevel(1)
    smtp.starttls()
    smtp.login("vpfa.reports@gmail.com","921211@Rx")
    smtp.sendmail("report",["vpfa.reports@gmail.com","nasir@virgilqr.com"], msg.as_string())
    #smtp.sendmail("report",["vpfa.reports@gmail.com"], msg.as_string())
    smtp.quit()

if __name__ == "__main__":
    volume_report()
    while True:
        time.sleep(60*60)
        try:
            volume_report()
        except: 
            time.sleep(60*60)
            pass
