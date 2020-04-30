import os 
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_logger import InfluxClient

db = InfluxClient()


def logger(table_name,error,tag=None):
    measurement = "errorlog"
    fields = {}
    fields.update({"error":error})
    fields.update({"symbol":tag})
    tags = {}
    tags.update({"table_name":table_name})
    tags.update({"dbhost":"host1"})
    time = False
    db.write_points_to_measurement(measurement, time, tags, fields)
    
