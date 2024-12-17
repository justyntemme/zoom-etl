import pandas
import pymysql
import sqlalchemy
import os
import json
#from pymysql import Connection
import pymysql
#from pymysql import cursors
from logging import Logger

AWS_DB_NAME = os.getenv("AWS_DB_NAME")
AWS_DB_SECRET_KEY = os.getenv("AWS_DB_SECRET_KEY")

#AWS_DB_SECRET = json.loads(get_aws_secret(AWS_DB_SECRET_KEY))
AWS_DB_USER = os.getenv('username')
AWS_DATABASE_PASSWORD = os.getenv('password')
AWS_DB_PORT =  os.getenv('port')
AWS_DATABASE = os.getenv('host')

def connect(logging: Logger):# -> pymysql.Connection[pymysql.Cursor]:
    try:
        cnx = pymysql.connect(host=AWS_DATABASE, user=AWS_DB_USER, port=int(AWS_DB_PORT), passwd=AWS_DATABASE_PASSWORD, db=AWS_DB_NAME)
    except Exception  as err:
        logging.error(err)
        exit(1)
    else:
        return cnx

def initialize_zoom_tables(logging: Logger):
    """ Create tables in the database"""
    command = """
        CREATE TABLE kri_data (
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
        
    try:
        conn = connect(logging)
        cur = conn.cursor()
        cur.execute(command)
    except Exception as e:
        logging.error("Database connection failed due to {}".format(e))


def test_zoom_tables(logging: Logger):
    """ Create tables in the database"""
    command = """
        SELECT * from kri_data LIMIT 10;
        """
    try:
        conn = connect(logging)
        cur = conn.cursor()
        cur.execute(command)
        body = cur.fetchall()
        logging.info("Returned Data: {}".format(json.dumps(body)))
    except Exception as e:
        logging.error("Database connection failed due to {}".format(e))

def run_sql_command(logging: Logger, command):
    try:
        conn = connect(logging)

        cur = conn.cursor()
        logging.info(command)
        cur.execute(command)
    except Exception as e:
        logging.error("Database connection failed due to {}".format(e))

def sqlalchemy_conn():
    ssl_args = {"ssl_ca": './global-bundle.pem'}
    url = sqlalchemy.engine.URL('mysql+pymysql', AWS_DB_USER, AWS_DATABASE_PASSWORD, AWS_DATABASE, int(AWS_DB_PORT), AWS_DB_NAME, {'charset': 'utf8mb4'})
    engine = sqlalchemy.create_engine(url, connect_args=ssl_args)
    return engine

def write_dataframe_to_sql(logging: Logger, dataFrame: pandas.DataFrame):
    try:
        conn= sqlalchemy_conn()
        logging.info("Writing data to sql...")

        dataFrame.to_sql(name='kri_data', con=conn, if_exists='append',index=False)
        logging.info("Data written! Closing connection...")
        conn.dispose()
        logging.info("Connection Closed!")
    except Exception as err:
        logging.error("Database connection failed due to {}".format(err))