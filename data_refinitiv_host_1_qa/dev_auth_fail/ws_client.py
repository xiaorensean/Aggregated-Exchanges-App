import traceback
import sys
import os
import time
import json
import websocket
import threading

current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from utility.error_logger_writer import logger

# Global Default Variables
app_id = '256'
position = ''
refresh_token = ''
service = 'ELEKTRON_DD'

# Global Variables
web_socket_app = None
web_socket_open = False
logged_in = False
host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()


class WebSocketMarketPrice:
    logged_in = False
    session_name = ''
    web_socket_app = None
    web_socket_open = False
    host = ''
    disconnected_by_user = False

    def __init__(self, host, sts_token, position, symbol):
        self.session_name = symbol
        self.host = host
        self.position = position
        self.sts_token = sts_token
        self.ric = symbol

    def _send_data_request(self, ric_name):
        """ Create and send simple Market Price request """
        mp_req_json = {
            "ID": 2,
            "Key": {
                "Name": ric_name
            },
        }
        self.web_socket_app.send(json.dumps(mp_req_json))
        print("SENT on " + self.session_name + ":")
        print(json.dumps(mp_req_json, sort_keys=True, indent=2, separators=(',', ':')))

    def _send_login_request(self, auth_token, is_refresh_token):
        """
            Send login request with authentication token.
            Used both for the initial login and subsequent reissues to update the authentication token
        """
        login_json = {
            'ID': 1,
            'Domain': 'Login',
            'Key': {
                'NameType': 'AuthnToken',
                'Elements': {
                    'ApplicationId': '',
                    'Position': '',
                    'AuthenticationToken': ''
                }
            }
        }

        login_json['Key']['Elements']['ApplicationId'] = app_id
        login_json['Key']['Elements']['Position'] = self.position
        login_json['Key']['Elements']['AuthenticationToken'] = auth_token

        # If the token is a refresh token, this is not our first login attempt.
        if is_refresh_token:
            login_json['Refresh'] = False

        self.web_socket_app.send(json.dumps(login_json))
        print("SENT on " + self.session_name + ":")
        print(json.dumps(login_json, sort_keys=True, indent=2, separators=(',', ':')))

    def _process_login_response(self, message_json):
        """ Send item request """
        if message_json['State']['Stream'] != "Open" or message_json['State']['Data'] != "Ok":
            print("Login failed.")
            sys.exit(1)

        self.logged_in = True
        self._send_data_request(self.ric)

    def _write_market_data(self,data, dt):
        ticker = self.ric
        if "=" in ticker:
            ticker = ticker.replace("=","_")
        else:
            pass
        measurement = "refinitiv_" + dt + "_" + ticker
        fields = data
        # write everything as float
        for key, value in fields.items():
            if type(value) == int:
                fields[key] = float(value)
        fields.update({"is_api_return_timestamp": True})
        dbtime = False
        tags = {}
        tags.update({"symbol":self.ric})
        host_1.write_points_to_measurement(measurement,dbtime,tags,fields)

    def _process_message(self, message_json):
        """ Parse at high level and output JSON of message """
        message_type = message_json['Type']

        if message_type == "Refresh":
            if 'Domain' in message_json:
                message_domain = message_json['Domain']
                if message_domain == "Login":
                    self._process_login_response(message_json)
        elif message_type == "Ping":
            pong_json = {'Type': 'Pong'}
            self.web_socket_app.send(json.dumps(pong_json))
            print("SENT on " + self.session_name + ":")
            print(json.dumps(pong_json, sort_keys=True, indent=2, separators=(',', ':')))

    # Callback events from WebSocketApp
    def _on_message(self, message):
        """ Called when message received, parse message into JSON for processing """
        print("RECEIVED on " + self.session_name + ":")
        message_json = json.loads(message)
        data = json.dumps(message_json, sort_keys=True, indent=2, separators=(',', ':'))

        for singleMsg in message_json:
            print(singleMsg)
            try:
                #print(singleMsg['UpdateType'], singleMsg['Fields'])
                data = singleMsg['Fields']
                data_type = singleMsg['UpdateType']
                try:
                    self._write_market_data(data, data_type)
                except:
                    error = traceback.format_exc()
                    print(error)
                    measurement = "refinitiv_" + data_type + "_" + self.ric
                    logger(measurement, error, self.ric)
            except:
                pass
            self._process_message(singleMsg)

    def _on_error(self, error):
        """ Called when websocket error has occurred """
        print(error + " for " + self.session_name)

    def _on_close(self):
        """ Called when websocket is closed """
        self.web_socket_open = False
        self.logged_in = False
        print("WebSocket Closed for " + self.session_name)

        if not self.disconnected_by_user:
            print("Reconnect to the endpoint for " + self.session_name + " after 3 seconds... ")
            time.sleep(3)
            self.connect()

    def _on_open(self):
        """ Called when handshake is complete and websocket is open, send login """

        print("WebSocket successfully connected for " + self.session_name + "!")
        self.web_socket_open = True
        self._send_login_request(self.sts_token, False)

    # Operations
    def connect(self):
        # Start websocket handshake
        ws_address = "wss://{}/WebSocket".format(self.host)
        print("Connecting to WebSocket " + ws_address + " for " + self.session_name + "...")
        self.web_socket_app = websocket.WebSocketApp(ws_address, on_message=self._on_message,
                                                     on_error=self._on_error,
                                                     on_close=self._on_close,
                                                     subprotocols=['tr_json2'])
        self.web_socket_app.on_open = self._on_open

        # Event loop
        wst = threading.Thread(target=self.web_socket_app.run_forever, kwargs={'sslopt': {'check_hostname': False}})
        wst.start()

    def disconnect(self):
        print("Closing the WebSocket connection for " + self.session_name)
        self.disconnected_by_user = True
        if self.web_socket_open:
            self.web_socket_app.close()

    def refresh_token(self):
        if self.logged_in:
            print("Refreshing the access token for " + self.session_name)
            self._send_login_request(self.sts_token, True)
