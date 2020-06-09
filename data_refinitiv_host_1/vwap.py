import sys
import os
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2
from utility.error_logger_writer import logger

host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()

table_name = host_1.get_table_name()
print(table_name)