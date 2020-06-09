
import os
from time import sleep
import sys
pkg_dir =  os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(pkg_dir)

import multiprocessing

from utility import resubscribe

processes = []

scripts = [
    "deribit_tickers_host_1_qa/subscribe_ticker_btc.py",
    "deribit_tickers_host_1_qa/subscribe_ticker_eth.py",

]

def create_process(directory, scriptname):
	process = multiprocessing.Process(target=resubscribe.run_forever, args=(directory, scriptname))
	process.start()

	entry = {
		"directory": directory,
		"scriptname": scriptname,
		"process": process
	}
	return entry

if __name__ == "__main__":
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




