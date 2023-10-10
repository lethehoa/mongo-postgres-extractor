from datetime import datetime
import json, os, csv
from bson.objectid import ObjectId

def generate_mongo_date_filter(from_date, to_date):
    if from_date == "" and to_date == "":
        return {}
    else:
        if from_date != "" and to_date == "":
            return {"_id": { "$gt": converter(from_date)}}
        elif from_date == "" and to_date != "":
            return {"_id": { "$lt": converter(to_date)}}
        else:
            return {"_id": {"$gt": converter(from_date), "$lt": converter(to_date)}}

def generate_update_on_conflict(columns, values):
    update_query = ""
    for i in range(len(columns)):
        if i == len(columns) - 1:
            update_query += columns[i] + "=" +"'{}'".format(values[i])
        else: update_query += columns[i] + "=" + "'{}'".format(values[i]) + ','
    return update_query

def generate_postgres_query(table_name):
    pass

def export_to_csv(json_source, csv_target):
    os.makedirs("final", exist_ok=True)
    with open(os.path.join("temp", json_source)) as json_file:
        count = 0
        with open(os.path.join("final", csv_target),"w") as final_file:
            csv_writer = csv.writer(final_file)
            for line in json_file:
                if count == 0:
                    csv_writer.writerow(json.loads(line).keys()) # write column name
                    count = 1
                csv_writer.writerow(json.loads(line).values()) # write value

def create_value_field(number_of_columns): # this fuction return one part of the query 
    value = ""
    for i in range(number_of_columns):
        if i == 0:
            value += "(%s,"
        if i == number_of_columns - 1:
            value += "%s)"
        else:
            value += "%s,"
    return value

def converter(date):
    # time_stamp = datetime.datetime.utcnow().strftime('%Y%m%d')
    date_object = datetime.strptime(date, '%m-%d-%Y')
    test = date_object.timestamp()
    id = hex(int(round(test)))[2:]

    # convert from normal day to timestamp

    # time_stamp_test = datetime.now().timestamp()
    # print(int(round(time_stamp_test)))
    return ObjectId(id + "0000000000000000")



