import psycopg2,os,csv
from mongo_exporter import CONFIG, schemas
from utils import *


def create_connection():
    print("-" * 20)
    print((f'Copying data to Postgres using {CONFIG["postgres"]["PG_HOST"]} DB {CONFIG["postgres"]["PG_DB"]} SCHEMA {CONFIG["postgres"]["PG_SCHEMA"]}'))
    conn = psycopg2.connect(
        host=CONFIG["postgres"]["PG_HOST"],
        port=CONFIG["postgres"]["PG_PORT"],
        dbname=CONFIG["postgres"]["PG_DB"],
        user=CONFIG["postgres"]["PG_USERNAME"],
        password=CONFIG["postgres"]["PG_PASSWORD"],
    )
    cursor = conn.cursor()
    return conn

def insert_DB():
    conn = create_connection()
    cursor = conn.cursor()
    columns = []
    print("-" * 20)
    print(f'Copying data for schema {CONFIG["postgres"]["PG_TABLE"]}.')

    print("Executing copy statement.")
    with open(os.path.join("final", f'{CONFIG["mongo"]["MONGO_COLLECTION"]}.csv'), "r") as final_file:
        reader = csv.reader(final_file)
        all_collumn = next(reader)
        for element in CONFIG["data_mapper"]:
            columns.append(CONFIG["data_mapper"][element])
        print(columns)
        value = create_value_field(len(all_collumn) - 1)
        for row in reader:
            print(row)
            # print(generate_update_on_conflict(columns, row))
            cursor.execute(f""" 
                INSERT INTO {CONFIG["postgres"]["PG_SCHEMA"]}.{CONFIG["postgres"]["PG_TABLE"]} {str(tuple(columns)).replace("'","")}
                VALUES {value} 
                on conflict ({columns[0]})
                do update
                set {generate_update_on_conflict(columns, row)};
                """, tuple(row)) 
            # print(cursor)

    conn.commit()
    conn.close()