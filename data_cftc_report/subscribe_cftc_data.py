import os
import sys
import pandas as pd
import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_host_1 import InfluxClientHost1
from influxdb_client.influxdb_client_host_2 import InfluxClientHost2


host_1 = InfluxClientHost1()
host_2 = InfluxClientHost2()


measurement = "test_cftc"#"cftc_futures_report"

def read_files(host,measurement):
    all_file = list(sorted([i for i in os.listdir(current_dir+"/deafut") if i != ".DS_Store"]))
    df = pd.read_excel(current_dir + "/deafut/" + all_file[-1])
    df_dict_raw = df.T.to_dict()
    data = [df_dict_raw[i] for i in df_dict_raw]
    write_cftc_report(data, host, measurement)


def write_cftc_report(df_dict,host,measurement):
    for data in df_dict:
        fields = {dd:data[dd] for dd in data if dd != "CFTC_Contract_Market_Code" and dd != "Report_Date_as_MM_DD_YYYY"}
        tags = {} 
        tags.update({"CFTC_Contract_Market_Code":data["CFTC_Contract_Market_Code"]})
        dbtime = False#datetime.datetime.strptime(str(df_dict[0]['Report_Date_as_MM_DD_YYYY']),"%Y-%m-%d %H:%M:%S")
        host.write_points_to_measurement(measurement, dbtime, tags, fields)


#all_file = list(sorted([i for i in os.listdir(current_dir+"/deafut") if i != ".DS_Store"]))
#df = pd.read_excel(current_dir + "/deafut/" + all_file[-1])
#df_dict_raw = df.T.to_dict()
#df_dict = [df_dict_raw[i] for i in df_dict_raw]
#dt = 


if __name__ == "__main__":
    read_files(host_1,measurement)
