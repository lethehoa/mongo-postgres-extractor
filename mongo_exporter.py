import csv
import json
import os
import pymongo
import configparser
import argparse

from typing import Dict
from relationalize import Relationalize, Schema
from relationalize.utils import create_local_file
from utils import *

CONFIG = configparser.ConfigParser()
CONFIG.optionxform=str

schemas: Dict[str, Schema] = {}

def read_config_file():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="-c or --config - pass config file's location", nargs='*')
    try:
        args = parser.parse_args().config[0]
    except:
        print("Option -h for help")
        exit(1)
    return args

CONFIG.read(read_config_file())

def export_flatJSON():
    os.makedirs("json", exist_ok=True)
    query = {}
    for element in CONFIG.options("data_mapper"):
        query[element] = 1
    MONGO_CONNECTION_STRING = f'mongodb://{CONFIG["mongo"]["MONGO_USER"]}:{CONFIG["mongo"]["MONGO_PASS"]}@{CONFIG["mongo"]["MONGO_HOST"]}:2717/?retryWrites=true&w=majority'
    # connect mongo
    collection = pymongo.MongoClient(MONGO_CONNECTION_STRING)[CONFIG["mongo"]["MONGO_DB"]][
        CONFIG["mongo"]["MONGO_COLLECTION"]
    ]
    filter_by_date = generate_mongo_date_filter(CONFIG["filter"]["from"], CONFIG["filter"]["to"])
    # write mongodb to file 
    with open(os.path.join("json", f"export.json"), "w") as export_file:
        for document in collection.find(filter_by_date,query):
            export_file.write(f"{json.dumps(document, default=str)}\n")

# this gets called when a relationalized object is written to the temporary file.
def on_object_write(schema: str, object: dict):
    if schema not in schemas:
        schemas[schema] = Schema()
    schemas[schema].read_object(object)

def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)

def export_csv_2():
    os.makedirs("temp", exist_ok=True)
    with Relationalize(CONFIG["mongo"]["MONGO_COLLECTION"], create_local_file("temp"), on_object_write) as r:
        r.relationalize(create_iterator("./json/export.json"))
    for schema_name, schema in schemas.items():
        print(f"Converting objects for schema {schema_name}.")
        export_to_csv(f"{schema_name}.json", f"{schema_name}.csv")

def export_csv():
    os.makedirs("temp", exist_ok=True)

    with Relationalize(CONFIG["mongo"]["MONGO_COLLECTION"], create_local_file("temp"), on_object_write) as r:
        r.relationalize(create_iterator("export.json"))

    print("-" * 20)
    print(f"Converting objects for {len(schemas)} relationalized schemas.")
    os.makedirs("final", exist_ok=True)
    for schema_name, schema in schemas.items():
        print(f"Converting objects for schema {schema_name}.")
        with open(
            os.path.join("final", f"{schema_name}.csv"),
            "w",
        ) as final_file:
            writer = csv.DictWriter(final_file, fieldnames=schema.generate_output_columns())
            writer.writeheader()
            for row in create_iterator(os.path.join("temp", f"{schema_name}.json")):
                converted_obj = schema.convert_object(row)
                writer.writerow(converted_obj)
