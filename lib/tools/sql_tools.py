import logging
import os
import argparse
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

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--init", help="init table with schema", action="store_true")
parser.add_argument(
    "-p", "--performance", help="test performance metrics", action="store_true"
)
args = parser.parse_args()

command_optimized = f"""
    CREATE TABLE {DB_TABLE} (
        id_serial INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        rrn VARCHAR(255),
        stateId VARCHAR(100),
        assetId VARCHAR(100),
        id VARCHAR(100),
        name VARCHAR(255),
        accountId VARCHAR(100),
        accountName VARCHAR(255),
        cloudType VARCHAR(50),
        regionId VARCHAR(100),
        regionName VARCHAR(255),
        service VARCHAR(100),
        resourceType VARCHAR(100),
        Passed VARCHAR(10),
        startDate DATETIME,
        endDate DATETIME,
        sheet_name VARCHAR(255),
        INDEX (rrn),
        INDEX (stateId),
        INDEX (assetId),
        INDEX (id),
        INDEX (accountId),
        INDEX (cloudType),
        INDEX (regionId),
        INDEX (service),
        INDEX (resourceType),
        INDEX (startDate)
    );
    """
command_original = f"""
    CREATE TABLE {DB_TABLE} (
        id_serial SERIAL PRIMARY KEY,
        rrn TEXT,
        stateId TEXT,
        assetId TEXT,
        id TEXT,
        name TEXT,
        accountId TEXT,
        accountName TEXT,
        cloudType TEXT,
        regionId TEXT,
        regionName TEXT,
        service TEXT,
        resourceType TEXT,
        Passed TEXT,
        startDate TIMESTAMP,
        endDate TIMESTAMP,
        sheet_name TEXT
    )
    """


def main():
    with db.DBAccessor(
        DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, int(DB_PORT), logger
    ) as db_accessor:
        if args.init:
            db_accessor.execute_query(command_optimized)
        if args.performance:
            logger.info("Running performance testing:\n")
            db_accessor.analyze_table(DB_TABLE)


if __name__ == "__main__":
    main()
