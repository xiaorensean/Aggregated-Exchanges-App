import os
import sys
import multiprocessing as mp
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from get_market_data import fetch_market_data


symbols = ["USc1", "TYc1", "FVc1", "TUc1", "1YMc1", "ESc1", "NQc1", "CLc1", "NGc1", "GCc1", "BTCc1",
           "NGc1", "HOc1", "BTC=", "BTC=BTSP", "LTC=BTSP", "ETH=BTSP", "BCH=BTSP", "XRP=BTSP"]

if __name__ == "__main__":
    fetch_market_data(symbols[0])
    #processes = {}
    #for symb in symbols:
    #    print(symb)
    #    data = mp.Process(target=fetch_market_data, args=(symb,))
    #    data.start()
    #    processes.update({symb:data})
