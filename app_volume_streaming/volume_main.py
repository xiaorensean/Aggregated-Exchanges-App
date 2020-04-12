import smtplib
import copy
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime 
import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_feed.binance_rest_feed import get_volume_binance
from data_feed.ftx_data_feed import get_volume_ftx
from data_feed.huobi_rest_feed import volume_report_huobi
from data_feed.huobidm_data_feed import volume_report_huobidm
from data_feed.okex_ws_feed import volume_report_okex
from influxdb_client import InfluxClient

pd.set_option('display.float_format', lambda x: '%.5f' % x)

db = InfluxClient()


bsv_okex = ["spot/ticker:BSV-USDT","spot/ticker:BSV-USDK"]
bch_okex = ["spot/ticker:BCH-USDT","spot/ticker:BCH-USDK"]

# get all volume data
bsv_binance = get_volume_binance('BSV')
bsv_ftx = get_volume_ftx('BSV')
bsv_huobi = volume_report_huobi('bsv')
bsv_huobidm = volume_report_huobidm('BSV')
bsv_okex = volume_report_okex(bsv_okex)
bch_binance = get_volume_binance('BCH')
bch_ftx = get_volume_ftx('BCH')
bch_huobi = volume_report_huobi('bch')
bch_huobidm = volume_report_huobidm('BCH')
bch_okex = volume_report_okex(bch_okex)

 
# writing vol record
bch_volume_total = bch_binance['total_binance_bch'] + bch_ftx['total_ftx_bch'] + bch_huobi['total_huobi_bch']+ bch_okex['total_okex_bch']+ bch_huobidm['total_huobidm_bch']
bsv_volume_total = bsv_binance['total_binance_bsv'] + bsv_ftx['total_ftx_bsv'] + bsv_huobi['total_huobi_bsv']+ bsv_okex['total_okex_bsv']+ bsv_huobidm['total_huobidm_bsv']
writting_obj = copy.copy(bsv_binance)
writting_obj.update(bsv_ftx)
writting_obj.update(bsv_okex)
writting_obj.update(bsv_huobi)
writting_obj.update(bsv_huobidm)
writting_obj.update(bch_binance)
writting_obj.update(bch_ftx)
writting_obj.update(bch_okex)
writting_obj.update(bch_huobi)
writting_obj.update(bch_huobidm)
writting_obj.update({"total_bch":bch_volume_total})
writting_obj.update({"total_bsv":bsv_volume_total})
measurement = "log_volume_report"
dbtime = False
tags = {}
fields = writting_obj
db.write_points_to_measurement(measurement, dbtime, tags, fields)

# generating vol changes data
# read from database 
read_db = db.query_tables(measurement,["*","order by time desc limit 2"])
# taking the previous vol log
read_logger = read_db.tail(1)
prev_volume = read_logger.T.to_dict()[1]

def get_vol_change(vol_dict,db_vol):
    prev_vol = {}
    key = vol_dict.keys()
    for k in key:
        prev_vol.update({k:db_vol[k]})
    vol_change = {}
    for k in key:
            vol_change.update({k:vol_dict[k] - prev_vol[k]})
    return vol_change

# get vol change data
bsv_binance_change = get_vol_change(bsv_binance,prev_volume)
bsv_ftx_change = get_vol_change(bsv_ftx,prev_volume)
bsv_huobi_change = get_vol_change(bsv_huobi,prev_volume)
bsv_huobidm_change = get_vol_change(bsv_huobidm,prev_volume)
bsv_okex_change = get_vol_change(bsv_okex,prev_volume)
bch_binance_change = get_vol_change(bch_binance,prev_volume)
bch_ftx_change = get_vol_change(bch_ftx,prev_volume)
bch_huobi_change = get_vol_change(bch_huobi,prev_volume)
bch_huobidm_change = get_vol_change(bch_huobidm,prev_volume)
bch_okex_change = get_vol_change(bch_okex,prev_volume)

# convert to df
bsv_binance_change_df = pd.DataFrame(bsv_binance_change,index=['Volume Change'])
bsv_ftx_change_df = pd.DataFrame(bsv_ftx_change,index=['Volume Change'])
bsv_huobi_change_df = pd.DataFrame(bsv_huobi_change,index=['Volume Change'])
bsv_huobidm_change_df = pd.DataFrame(bsv_huobidm_change,index=['Volume Change'])
bsv_okex_change_df = pd.DataFrame(bsv_okex_change,index=['Volume Change'])
bch_binance_change_df = pd.DataFrame(bch_binance_change,index=['Volume Change'])
bch_ftx_change_df = pd.DataFrame(bch_ftx_change,index=['Volume Change'])
bch_huobi_change_df = pd.DataFrame(bch_huobi_change,index=['Volume Change'])
bch_huobidm_change_df = pd.DataFrame(bch_huobidm_change,index=['Volume Change'])
bch_okex_change_df = pd.DataFrame(bch_okex_change,index=['Volume Change'])

current_timestamp = str(datetime.datetime.utcnow())
bsv_binance_vol = pd.DataFrame(bsv_binance,index=['Volume'])
bsv_ftx_vol = pd.DataFrame(bsv_ftx,index=['Volume'])
bsv_huobi_vol = pd.DataFrame(bsv_huobi,index=['Volume'])
bsv_huobidm_vol = pd.DataFrame(bsv_huobidm,index=['Volume'])
bsv_okex_vol = pd.DataFrame(bsv_okex,index=['Volume'])
bch_binance_vol = pd.DataFrame(bch_binance,index=['Volume'])
bch_ftx_vol = pd.DataFrame(bch_ftx,index=['Volume'])
bch_huobi_vol = pd.DataFrame(bch_huobi,index=['Volume'])
bch_huobidm_vol = pd.DataFrame(bch_huobidm,index=['Volume'])
bch_okex_vol = pd.DataFrame(bch_okex,index=['Volume'])
bsv_binance_df = pd.concat([bsv_binance_vol,bsv_binance_change_df])
bsv_ftx_df = pd.concat([bsv_ftx_vol,bsv_ftx_change_df])
bsv_huobi_df = pd.concat([bsv_huobi_vol,bsv_huobi_change_df])
bsv_huobidm_df = pd.concat([bsv_huobidm_vol,bsv_huobidm_change_df])
bsv_okex_df = pd.concat([bsv_okex_vol,bsv_okex_change_df])
bch_binance_df = pd.concat([bch_binance_vol,bch_binance_change_df])
bch_ftx_df = pd.concat([bch_ftx_vol,bch_ftx_change_df])
bch_huobi_df = pd.concat([bch_huobi_vol,bch_huobi_change_df])
bch_huobidm_df = pd.concat([bch_huobidm_vol,bch_huobidm_change_df])
bch_okex_df = pd.concat([bch_okex_vol,bch_okex_change_df])

summary = pd.DataFrame([[bch_volume_total,bsv_volume_total],
                        [bch_volume_total-prev_volume['total_bch'],
                         bsv_volume_total-prev_volume['total_bsv']]])
summary.columns = ['BCH',"BSV"]
summary.index = ['Volume','Volume Change']

# send email with tables
html_summary = summary.to_html()
html_binance_bch = bch_binance_df.to_html()
html_ftx_bch = bch_ftx_df.to_html()
html_huobi_bch = bch_huobi_df.to_html()
html_huobidm_bch = bch_huobidm_df.to_html()
html_okex_bch = bch_okex_df.to_html()
html_binance_bsv = bsv_binance_df.to_html()
html_ftx_bsv = bsv_ftx_df.to_html()
html_huobi_bsv = bsv_huobi_df.to_html()
html_huobidm_bsv = bsv_huobidm_df.to_html()
html_okex_bsv = bsv_okex_df.to_html()
msg = MIMEMultipart()
msg['Subject'] = "BCH & BSV Volume Report"
msg['From'] = 'xiao@virgilqr.com'

html = """\
    <html>
      <head></head>
      <body>
        <p> Current Timestamp: {0}  
        
          <br>Summary for BCH & BSV Across Major Exchanges:<br>
           {1}
           
           <br>Binance BCH:<br>
           {2}
           <br>FTX BCH:<br>
           {3}
           <br>Huobi BCH:<br>
           {4}
           <br>HuobiDM BCH:<br>
           {5}
           <br>Okex BCH:<br>
           {6}
           <br>Binance BSV:<br>
           {7}
           <br>FTX BSV:<br>
           {8}
           <br>Huobi BSV:<br>
           {9}
           <br>HuobiDM BSV:<br>
           {10}
           <br>Okex BSV:<br>
           {11}
           
        </p>
      </body>
    </html>
          """.format(current_timestamp,html_summary,html_binance_bch,html_ftx_bch,
          html_huobi_bch,html_huobidm_bch, html_okex_bch,html_binance_bsv,
          html_ftx_bsv,html_huobi_bsv, html_huobidm_bsv,html_okex_bsv)

part1 = MIMEText(html, 'html')
msg.attach(part1)

smtp = smtplib.SMTP('smtp.gmail.com',587)
smtp.starttls()
smtp.login("xiao@virgilqr.com","921211Rx")
smtp.sendmail("monitor",["xiao@virgilqr.com","nasir@virgilqr.com"], msg.as_string())
smtp.quit()