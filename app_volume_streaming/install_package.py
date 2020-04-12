import subprocess
import sys
import argparse

# function to install package
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

parser = argparse.ArgumentParser(description='install packages')
    
# argument to install package
parser.add_argument('--install', metavar='package_name', required=False,
        default=None, type=str)

# get parse_args object
opts = parser.parse_args()
if opts.install is not None:
    install(opts.install)
else:
    print("No package to install")
