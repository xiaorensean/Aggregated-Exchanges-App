import subprocess
import time
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


def run_job(directory, script):
    while True:
        try:
            check_output = subprocess.check_output(['python3', script], cwd = directory)
            print (check_output)
            time.sleep(60*60*24)
        except KeyboardInterrupt:
            break

    
script = 'volume_main.py'
run_job(current_dir,script)
