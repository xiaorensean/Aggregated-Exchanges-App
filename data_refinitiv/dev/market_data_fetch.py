import os
import sys
import time
import socket
import requests
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from auth import get_sts_token
from ws_market_data import WebSocketMarketPrice

# Global varibles
hotstandby = False
session2 = None
region = "amer"
hostList = []
original_expire_time = '0'; 

def query_service_discovery(url=None):
    discovery_url = 'https://api.refinitiv.com/streaming/pricing/v1/'
    if url is None:
        url = discovery_url

    print("Sending EDP-GW service discovery request to " + url)

    try:
        r = requests.get(url, headers={"Authorization": "Bearer " + sts_token}, params={"transport": "websocket"}, allow_redirects=False)

    except requests.exceptions.RequestException as e:
        print('EDP-GW service discovery exception failure:', e)
        return False

    if r.status_code == 200:
        # Authentication was successful. Deserialize the response.
        response_json = r.json()
        print("EDP-GW Service discovery succeeded. RECEIVED:")
        print(json.dumps(response_json, sort_keys=True, indent=2, separators=(',', ':')))

        for index in range(len(response_json['services'])):

            if region == "amer":
                if not response_json['services'][index]['location'][0].startswith("us-"):
                    continue
            elif region == "emea":
                if not response_json['services'][index]['location'][0].startswith("eu-"):
                    continue
            elif region == "apac":
                if not response_json['services'][index]['location'][0].startswith("ap-"):
                    continue

            if not hotstandby:
                if len(response_json['services'][index]['location']) == 2:
                    hostList.append(response_json['services'][index]['endpoint'] + ":" +
                                    str(response_json['services'][index]['port']))
                    break
            else:
                if len(response_json['services'][index]['location']) == 1:
                    hostList.append(response_json['services'][index]['endpoint'] + ":" +
                                    str(response_json['services'][index]['port']))

        if hotstandby:
            if len(hostList) < 2:
                print("hotstandby support requires at least two hosts")
                sys.exit(1)
        else:
            if len(hostList) == 0:
                print("No host found from EDP service discovery")
                sys.exit(1)

        return True

    elif r.status_code == 301 or r.status_code == 302 or r.status_code == 303 or r.status_code == 307 or r.status_code == 308:
        # Perform URL redirect
        print('EDP-GW service discovery HTTP code:', r.status_code, r.reason)
        new_host = r.headers['Location']
        if new_host is not None:
            print('Perform URL redirect to ', new_host)
            return query_service_discovery(new_host)
        return False
    elif r.status_code == 403 or r.status_code == 451:
        # Stop trying with the request
        print('EDP-GW service discovery HTTP code:', r.status_code, r.reason)
        print('Stop trying with the request')
        return False
    else:
        # Retry the service discovery request
        print('EDP-GW service discovery HTTP code:', r.status_code, r.reason)
        print('Retry the service discovery request')
        return query_service_discovery()




symbol = '/TRI.N'
sts_token, refresh_token, expire_time = get_sts_token(None)
if not sts_token:
    sys.exit(1)

original_expire_time = expire_time

# Query VIPs from EDP service discovery
if not query_service_discovery():
    print("Failed to retrieve endpoints from EDP Service Discovery. Exiting...")
    sys.exit(1)

# Start websocket handshake; create two sessions when the hotstandby parameter is specified.
session1 = WebSocketMarketPrice('Session 1',hostList[0],symbol,sts_token)
session1.connect()
'''
if hotstandby:
    session2 = WebSocketMarketPrice(hostList[1],symbol)
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
'''