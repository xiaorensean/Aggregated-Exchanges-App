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

def get_market_data(symbol):
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
    session1 = WebSocketMarketPrice('Session 1',hostList[0], sts_token, position, symbol)
    session1.connect()

    if hotstandby:
        session2 = WebSocketMarketPrice('Session 2',hostList[1], sts_token, position, symbol)
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
    symbol = "1YMc1"
    get_market_data(symbol)
