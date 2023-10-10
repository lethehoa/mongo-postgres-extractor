import csv
import json
import os
import psycopg2
import pymongo

from typing import Dict
from relationalize import Relationalize, Schema
from relationalize.utils import create_local_file



# MONGO_HOST = "localhost"
# MONGO_DB = "sample_supplies"
# MONGO_COLLECTION = "sales"

# PG_HOST = "localhost"
# PG_PORT = 5432
# PG_USERNAME = "postgres"
# PG_PASSWORD = "hoalt"
# PG_DB = "postgres"
# PG_SCHEMA = "public"

# MONGO_CONNECTION_STRING = f"mongodb://mongoa:test@localhost:2717/?retryWrites=true&w=majority"

# # connect mongo
# print("-" * 20)
# print(f"Exporting {MONGO_COLLECTION} from {MONGO_HOST} into export.json")
# restaurants_collection = pymongo.MongoClient(MONGO_CONNECTION_STRING)[MONGO_DB][
#     MONGO_COLLECTION
# ]

# # write mongodb to file 
# with open("export-customers.json", "w") as export_file:
#     for document in restaurants_collection.find({'name': 'Karen Wise'},{ "_id": 1, "name": 1, "address": 1 }):
#         export_file.write(f"{json.dumps(document, default=str)}\n")

# schemas: Dict[str, Schema] = {}

# # this gets called when a relationalized object is written to the temporary file.
# def on_object_write(schema: str, object: dict):
#     if schema not in schemas:
#         schemas[schema] = Schema()
#     schemas[schema].read_object(object)

# def create_iterator(filename):
#     with open(filename, "r") as infile:
#         for line in infile:
#             yield json.loads(line)


# print("-" * 20)
# print(f"Relationalizing {MONGO_COLLECTION} from local file export.json")
# os.makedirs("temp", exist_ok=True)

# with Relationalize(MONGO_COLLECTION, create_local_file("temp"), on_object_write) as r:
#     r.relationalize(create_iterator("export-customers.json"))

# print("-" * 20)
# print(f"Converting objects for {len(schemas)} relationalized schemas.")
# os.makedirs("final", exist_ok=True)
# for schema_name, schema in schemas.items():
#     print(f"Converting objects for schema {schema_name}.")
#     with open(
#         os.path.join("final", f"{schema_name}.csv"),
#         "w",
#     ) as final_file:
#         writer = csv.DictWriter(final_file, fieldnames=schema.generate_output_columns())
#         writer.writeheader()
#         for row in create_iterator(os.path.join("temp", f"{schema_name}.json")):
#             converted_obj = schema.convert_object(row)
#             writer.writerow(converted_obj)

# print("-" * 20)
# print((f"Copying data to Postgres using {PG_HOST} " f"DB {PG_DB} SCHEMA {PG_SCHEMA}"))
# conn = psycopg2.connect(
#     host=PG_HOST,
#     port=PG_PORT,
#     dbname=PG_DB,
#     user=PG_USERNAME,
#     password=PG_PASSWORD,
# )

# cursor = conn.cursor()

# for schema_name, schema in schemas.items():
#     print("-" * 20)
#     print(f"Copying data for schema {schema_name}.")
#     drop_table_statement = f'DROP TABLE IF EXISTS "{PG_SCHEMA}"."{schema_name}";'
#     create_table_statement = schema.generate_ddl(table=schema_name, schema=PG_SCHEMA)
#     print(create_table_statement)
#     # print("Executing drop table statement.")
#     # cursor.execute(drop_table_statement)
#     # conn.commit()

#     print("Executing create table statement.")
#     cursor.execute(create_table_statement)
#     conn.commit()

#     print("Executing copy statement.")
#     with open(os.path.join("final", f"{schema_name}.csv"), "r") as final_file:
#         reader = csv.reader(final_file)
#         allCollumn = next(reader)
#         print(allCollumn)
#         for row in reader:
#             print(row)
#             cursor.execute(f""" 
#                 INSERT INTO {schema_name} ({allCollumn[0]}, {allCollumn[1]}, {allCollumn[2]}) 
#                 VALUES (%s, %s, %s) 
#                 """, (row[0], row[1], row[2])) 

#         # cursor.copy_expert(
#         #     f"COPY {PG_SCHEMA}.{schema_name} from STDIN DELIMITER ',' CSV HEADER;",
#         #     final_file,
#         # )
#     conn.commit()

# print("-" * 20)
# print("Pipeline Complete")