'''
Runs one script forever, restarting script as soon as it terminates for whatever reason
'''

import subprocess

def run_forever(directory, script):
    while True:
        try:
            check_output = subprocess.check_output(['python3', script], cwd = directory)
            print (check_output)
            with open("logger.txt","w") as f:
                f.writelines(list(str(check_output)))
        except KeyboardInterrupt:
            break