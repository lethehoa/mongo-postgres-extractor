from mongo_exporter import *
from postgres_import import *

from relationalize import Relationalize, Schema
from relationalize.utils import create_local_file

print("-" * 20)
export_flatJSON()
print("-" * 20)
print(f"Converting objects for {len(schemas)} relationalized schemas.")
export_csv_2()
# export_csv()
# print("-" * 20)
# print((f"Copying data to Postgres using {PG_HOST} " f"DB {PG_DB} SCHEMA {PG_SCHEMA}"))
insert_DB()
print("-" * 20)
print("Pipeline Complete")