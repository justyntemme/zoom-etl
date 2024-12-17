import logging
import csv
import os
from ..src.db import db

from dotenv import load_dotenv

## NOTE: For now, run `fake_data_generator.py` first, to generate the `.csv`. Can easily optimize that flow, but for now it works.

# Assume the '.env' file is in root
load_dotenv("../../.env")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
DB_HOST = os.getenv("DB_HOST")
DB_TABLE = os.getenv("DB_TABLE")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_path = "lib/tools/data.csv"


def main():
    with open(file_path, newline="") as file:
        # Run the data generator and produce the file
        data = csv.reader(file)

        # Read the first row 'headers'
        header = next(data)

        # Do DB work
        with db.DBAccessor(
            DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, int(DB_PORT), logger
        ) as db_accessor:
            print("Inserting data")
            for row in data:
                db_accessor.execute_query(
                    f"INSERT INTO {DB_TABLE} ({', '.join(header)}) VALUES ({', '.join('\'{0}\''.format(w) for w in row)});"
                )

            print("Dumping data")
            rows = db_accessor.execute_query("SELECT * FROM kri_data")
            for row in rows:
                print(row)


if __name__ == "__main__":
    main()
