# input: data (time)

# default is this if time => datetime

from datetime import datetime
from bson.objectid import ObjectId
import configparser
import argparse
import csv, json,os
from utils import *

def converter(date):
    # time_stamp = datetime.datetime.utcnow().strftime('%Y%m%d')
    date_object = datetime.strptime(date, '%m-%d-%Y')
    test = date_object.timestamp()
    id = hex(int(round(test)))[2:]
    # convert from normal day to timestamp
    # time_stamp_test = datetime.now().timestamp()
    # print(int(round(time_stamp_test)))
    return ObjectId(id + "0000000000000000")

# print(generate_update_on_conflict(("_id","couponUsed","customer_email","customer_gender"), ("65228165a6f28d5e9518f6fe","True","hoalt2@sendo.vn","FM")))

os.makedirs("final", exist_ok=True)
with open('temp/sales.json') as json_file:
    count = 0
    with open(os.path.join("final", "sales.csv"),"w") as final_file:
        csv_writer = csv.writer(final_file)
        for line in json_file:
            if count == 0:
                header = json.loads(line).keys()
                csv_writer.writerow(header)
                count = 1
            csv_writer.writerow(json.loads(line).values())

# done convert file to json => start using it to standized json

# flatJSON, loop through file => each line is a single json => convert each json line to dict
# =>write to file
