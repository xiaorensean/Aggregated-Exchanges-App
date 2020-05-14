import websocket
import json
from termcolor import colored
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__))))


def subscribe(suffix = 'instrument,funding,insurance,settlement,liquidation,trad,chat'):
	connect_string = 'wss://www.bitmex.com/realtime?subscribe=' + suffix
	ws = websocket.create_connection(connect_string)

	while (True):
		res = ws.recv()
		response = None
		try:
			response = json.loads(res)
		except:
			continue

		endpoint = None
		data = None
		action = None
		try:
			endpoint = response["table"]
			data = response["data"]
			action = response["action"]
		except:
			try:
				if not response["success"]:
					print(colored("ERROR", "red"))
				continue
			except:
				continue


def subscribe_slow(suffix = 'instrument,funding,insurance,settlement,liquidation,trade,chat'):
	connect_string = 'wss://www.bitmex.com/realtime?subscribe=' + suffix
	ws = websocket.WebSocketApp(connect_string,
		on_message = on_message_subscribe_slow,
        on_error = on_error_subscribe_slow,
        on_close = on_close_subscribe_slow)
	ws.run_forever(ping_interval=60, ping_timeout=10)
	
    
def on_message_subscribe_slow(ws, message):
    response = None
    try:
        response = json.loads(message)
    except:
        return
    endpoint = None
    data = None
    action = None
    try:
        endpoint = response["table"]
        data = response["data"]
        print(data)
        action = response["action"]
    except:
        try:
            if not response["success"]:
                print(colored("ERROR", "red"))
                return
        except:
            return



def on_error_subscribe_slow(ws, error):
	sys.exit(1)


def on_close_subscribe_slow(ws):
    sys.exit(1)


if __name__ == "__main__":
    subscribe_slow('instrument')




