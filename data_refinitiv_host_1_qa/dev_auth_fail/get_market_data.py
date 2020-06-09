import os
import sys
import time
import socket

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from auth import get_sts_token
from ws_client import WebSocketMarketPrice
from get_url import query_service_discovery

# global variable
original_expire_time = '0'
hotstandby = False

def fetch_market_data(symbol):
    session2 = None
    position = ''
    if position == '':
        # Populate position if possible
        try:
            position_host = socket.gethostname()
            position = socket.gethostbyname(position_host) + "/" + position_host
        except socket.gaierror:
             position = "127.0.0.1/net"

    sts_token, refresh_token, expire_time = get_sts_token(None)
    if not sts_token:
        sys.exit(1)

    original_expire_time = expire_time

    # Query VIPs from EDP service discovery
    resp = query_service_discovery(sts_token)
    hostList = resp[1]

    # Start websocket handshake; create two sessions when the hotstandby parameter is specified.
    session1 = WebSocketMarketPrice(hostList[0], sts_token, position, symbol)
    session1.connect()

    if hotstandby:
        session2 = WebSocketMarketPrice(hostList[1], sts_token, position, symbol)
        session2.connect()

    try:
        while True:
            #  Continue using current token until 90% of initial time before it expires.
            time.sleep(int(float(expire_time) * 0.90))

            sts_token, refresh_token, expire_time = get_sts_token(refresh_token)
            if not sts_token:
                sys.exit(1)
            
            if int(expire_time) != int(original_expire_time):
                print('expire time changed from ' + str(original_expire_time) + ' sec to ' + str(expire_time) + ' sec; retry with password')
                sts_token, refresh_token, expire_time = get_sts_token(None)
                if not sts_token:
                    sys.exit(1)
                original_expire_time = expire_time

            # Update token.
            session1.refresh_token()
            if hotstandby:
                session2.refresh_token()

    except KeyboardInterrupt:
        session1.disconnect()
        if hotstandby:
            session2.disconnect()


if __name__ == "__main__":
    symbol = "BTC=BTSP"
    #symbol = "BTC=BTSP"
    #symbol = "BTC="
    fetch_market_data(symbol)

# 1YMc1
# {'PRC_QL_CD': 'OPN', 'BID_TIM_MS': 5013740, 'QUOTIM_MS': 5013740, 'BIDSIZE': 4, 'BID': 26271, 'BID_TIME1': '01:23:33', 'BID_COND_N': '0', 'NO_BIDORD1': 4, 'SPARE_TS1': '01:23:33', 'MKT_ST_IND': 'BBO', 'SEQNUM': 34320207, 'SEQNUM_QT': 34320207, 'ASK': 26273, 'ASKSIZE': 3, 'ASK_TIME1': '01:23:33', 'ASK_TIM_MS': 5013739, 'ASK_COND_N': '1', 'NO_ASKORD1': 3}
# BTC=BTSP
# {'TRDPRC_1': 9810.26, 'TRADE_DATE': '2020-06-05', 'SALTIM': '01:25:03', 'TRADE_ID': '115053442', 'VOL_DEC': 3.50187712, 'NUM_MOVES': 652, 'NETCHNG_1': 18.34, 'PCTCHNG': 0.19, 'AC