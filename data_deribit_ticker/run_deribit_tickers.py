'''
Main script
Spawns individual scripts that collect data from exchanges
Constantly checks that all the scripts are running, if any script stops for whatever reason, restart it
'''


import os
from time import sleep
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import multiprocessing

from utility import resubscribe

processes = []

scripts = [
    "deribit_tickers_host_1/subscribe_ticker_btc.py",
    "deribit_tickers_host_1/subscribe_ticker_eth.py",

]

def create_process(directory, scriptname):
	process = multiprocessing.Process(target=resubscribe.run_forever, args=(directory, scriptname))
	process.start()

	with open(process_logger, "a+") as f:
		lines = ['----------\n', directory + "/" + scriptname + "\n", str(process.pid) + "\n", '----------\n']
		f.writelines(lines)
	entry = {
		"directory": directory,
		"scriptname": scriptname,
		"process": process
	}
	return entry

if __name__ == "__main__":
	open(process_logger, 'w').close()
	restart = False

	for script in scripts:
		directory = script.split("/")[0]
		scriptname = script.split("/")[1]
		processes.append(create_process(directory, scriptname))

	while True:

		for entry in processes:
			process = entry["process"]
			directory = entry["directory"]
			scriptname = entry["scriptname"]
			if not process.is_alive():
				entry["process"].join()
				processes.remove(entry)
				processes.append(create_process(directory, scriptname))
				print(directory + "/" + scriptname )
		sleep(1)
   



