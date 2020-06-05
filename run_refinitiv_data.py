'''
Main script
Spawns individual scripts that collect data from exchanges
Constantly checks that all the scripts are running, if any script stops for whatever reason, restart it
'''
import os
from time import sleep
import sys
sys.path.append(os.path.join(os.path.dirname(__file__)))

import multiprocessing
from utility import resubscribe

process_logger = "logger/refinitiv_data2.txt"

processes = []

scripts = [
    "data_refinitiv_host_2/subscribe_data_1YMc1.py",
    "data_refinitiv_host_2/subscribe_data_CLc1.py",
    "data_refinitiv_host_2/subscribe_data_ESc1.py",
    "data_refinitiv_host_2/subscribe_data_FVc1.py",
	"data_refinitiv_host_2/subscribe_data_GCc1.py",
	"data_refinitiv_host_2/subscribe_data_HOc1.py",
	"data_refinitiv_host_2/subscribe_data_NGc1.py",
	"data_refinitiv_host_2/subscribe_data_NQc1.py",
	"data_refinitiv_host_2/subscribe_data_TUc1.py",
	"data_refinitiv_host_2/subscribe_data_TYc1.py",
	"data_refinitiv_host_2/subscribe_data_USc1.py",
	"data_refinitiv_host_2/subscribe_data_BTCc1.py",
	"data_refinitiv_host_2/subscribe_data_BTC_BTSP.py",
    "data_refinitiv_host_2/subscribe_data_BCH_BTSP.py",
	"data_refinitiv_host_2/subscribe_data_ETH_BTSP.py",
	"data_refinitiv_host_2/subscribe_data_LTC_BTSP.py",
	"data_refinitiv_host_2/subscribe_data_XRP_BTSP.py"
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
   



