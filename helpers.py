import os
import requests
import json
import logging
import boto3
import botocore 
import botocore.session 
import datetime as dt
import pandas as pd
import pymysql
import sqlalchemy
import traceback
from io import BytesIO
from dotenv import load_dotenv
from time import sleep
import datetime
import calendar
from boto3 import exceptions
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

load_dotenv()

# Global Variables
CSPM_ENDPOINT = os.getenv("CSPM_ENDPOINT")
CWPP_ENDPOINT = os.getenv("CWPP_ENDPOINT")
ACCESS_KEY_NAME = os.getenv("PRISMA_ACCESS_KEY")
SECRET_KEY_NAME = os.getenv("PRISMA_SECRET_KEY")
AWS_BUCKET=os.getenv("AWS_BUCKET")
AWS_REGION= os.getenv("AWS_REGION")

AWS_DB_NAME = os.getenv("AWS_DB_NAME")
AWS_DB_SECRET_KEY = os.getenv("AWS_DB_SECRET_KEY")

PRISMA_KEY= os.getenv("PRISMA_KEY")

SLEEP_AMOUNT=5

def get_aws_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=AWS_REGION
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']

    return secret

PRISMA_API_KEY = json.loads(get_aws_secret(PRISMA_KEY))
ACCESS_KEY = PRISMA_API_KEY['AccessKey']
SECRET_KEY = PRISMA_API_KEY['SecretKey']

AWS_DB_SECRET = json.loads(get_aws_secret(AWS_DB_SECRET_KEY))
AWS_DB_USER = AWS_DB_SECRET['username']
AWS_DATABASE_PASSWORD = AWS_DB_SECRET['password']
AWS_DB_PORT =  AWS_DB_SECRET['port']
AWS_DATABASE = AWS_DB_SECRET['host']

TODAYS_DATE = str(dt.datetime.today()).split()[0]
error_cache = list()

def mysql_connection():
    try:
        cnx = pymysql.connect(host=AWS_DATABASE, user=AWS_DB_USER, port=int(AWS_DB_PORT), passwd=AWS_DATABASE_PASSWORD, db=AWS_DB_NAME)
    except Exception  as err:
        logging.error(err)
        exit(1)
    else:
        return cnx

def initialize_zoom_tables():
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
            sheet_name TEXT,
            accountGroup TEXT
        )
        """

    try:
        conn = mysql_connection()
        cur = conn.cursor()
        cur.execute(command)
    except Exception as e:
        logging.error("Database connection failed due to {}".format(e))

def test_zoom_tables():
    """ Create tables in the database"""
    command = """
        SELECT * from kri_data LIMIT 10;
        """
    try:
        conn = mysql_connection()
        cur = conn.cursor()
        cur.execute(command)
        body = cur.fetchall()
        logging.info("Returned Data: {}".format(json.dumps(body)))
    except Exception as e:
        logging.error("Database connection failed due to {}".format(e))

def run_sql_command(command):
    try:
        conn = mysql_connection()

        cur = conn.cursor()
        logging.info(command)
        cur.execute(command)
    except Exception as e:
        logging.error("Database connection failed due to {}".format(e))

def sqlalchemy_conn():
    ssl_args = {"ssl_ca": './global-bundle.pem'}
    url = sqlalchemy.engine.URL('mysql+pymysql', AWS_DB_USER, AWS_DATABASE_PASSWORD, AWS_DATABASE, 3306, AWS_DB_NAME, {'charset': 'utf8mb4'})
    engine = sqlalchemy.create_engine(url, connect_args=ssl_args)
    return engine

def write_dataframe_to_sql(dataFrame):
    try:
        conn= sqlalchemy_conn()
        logging.info("Writing data to sql...")

        dataFrame.to_sql(name='kri_data', con=conn, if_exists='append',index=False)
        logging.info("Data written! Closing connection...")
        conn.dispose()
        logging.info("Connection Closed!")
    except Exception as err:
        logging.error("Database connection failed due to {}".format(err))

def write_buffer_to_s3(key,buffer):
    logger.info("Writing object to s3")
    s3 = boto3.resource('s3')
    s3.Object(AWS_BUCKET,key).put(Body=buffer.getvalue())

def generate_prisma_token(access_key: str, secret_key: str, cspm_endpoint: str) -> str:
    endpoint = f"https://{cspm_endpoint}/login"

    logger.info("Generating PRISMA token using endpoint: " + endpoint)

    headers = {
        "accept": "application/json; charset=UTF-8",
        "content-type": "application/json",
    }

    body = {"username": access_key, "password": secret_key}

    response = requests.post(endpoint, headers=headers, json=body)
    logging.info("generate_prisma_token Status Code: " + str(response.status_code))
    data = json.loads(response.text)

    return data["token"]

def fetch_account_group_data(token: str, cspm_endpoint: str) -> list:
    endpoint = f"https://{cspm_endpoint}/cloud/group/api/v1/account-group"

    logger.info("Fetching account group data using endpoint")

    headers = {
        "accept": "application/json; charset=UTF-8",
        "content-type": "application/json; charset=UTF-8",
        "x-redlock-auth": token,
    }

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        data = json.loads(response.text)

        return data
    else:
        logging.error(f"fetch_account_group_data returned status code: " + str(response.status_code))
        return None

def prisma_get_config_rql_search(token: str, cspm_endpoint: str, payload: dict) -> list:
    """
    Returns an config rql search.

    Parameters:
    token (str): Prisma tokent
    cspm_endpoint (str): Prisma CSPM API endpoint

    Returns:
    list: rql search result

    """
    endpoint = f"https://{cspm_endpoint}/search/api/v1/config"

    logger.info("Gathering config rql search using endpoint: " + endpoint)

    headers = {
        "accept": "application/json; charset=UTF-8",
        "content-type": "application/json; charset=UTF-8",
        "x-redlock-auth": token,
    }

    response = requests.post(endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        data = json.loads(response.text)

        return data, response.status_code
    elif response.status_code == 401:
        return None, 401
    elif response.status_code == 500:
        return None, 500

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)

        if response.status_code == 200:
            data = json.loads(response.text)

            return data, 200
        elif response.status_code == 401:
            return None, 401
        elif response.status_code == 503:
            return None, 503
        elif response.status_code == 400:
            return None, 400
        else:
            logging.error(f"prisma_get_config_rql_search returned status code: " + str(response.status_code))
            return None, response.status_code

    except requests.ReadTimeout:
        #logger.error(
        #    f"API returned response: {response.status_code}..."
        #)
        return None, 504
    except requests.exceptions.RequestException as err:
        logging.error("prisma_get_config_rql_search returned an error",err)
    except Exception as e:
        logging.error(traceback.format_exc())

def prisma_get_compared_rql_queries_dated(query1, query2, date, end_date):
        today = datetime.datetime.now()
        startdate = date
        dataFrame=pd.DataFrame()
        while(end_date >= startdate):
            enddate = startdate + datetime.timedelta(days=1) -datetime.timedelta(seconds=1)
            timeRange =  {
                "type": "absolute",
                "value": { "startTime": calendar.timegm(startdate.timetuple()) * 1000,
                        "endTime": calendar.timegm(enddate.timetuple()) * 1000
                }
            }
          
            rql_search_data_failed = prisma_extract_config_rql_search_timerange(query1,timeRange)
            rql_search_data_total = prisma_extract_config_rql_search_timerange(query2, timeRange)
            dataFrameFailed=pd.DataFrame(rql_search_data_failed)
            dataFrameTotal=pd.DataFrame(rql_search_data_total)

            dataFrameTemp=pd.DataFrame()
            if not dataFrameFailed.empty:
                df=pd.merge(dataFrameTotal,dataFrameFailed, how='outer', indicator=True)
                dfTotal=df.loc[df._merge == 'left_only'].drop("_merge",axis=1)
                dfTotal['Passed'] = True
                dataFrameFailed['Passed'] = False
                dataFrameTemp=pd.concat([dfTotal,dataFrameFailed])
            else:
                dataFrameTotal['Passed'] = True
                dataFrameTemp=dataFrameTotal

            dataFrameTemp['startDate'] = startdate
            dataFrameTemp['endDate'] = enddate
            dataFrame=pd.concat([dataFrame, dataFrameTemp])

            startdate= startdate + datetime.timedelta(days=1) 

        dataFrame['insertTs'] = pd.to_datetime(dataFrame['insertTs'] , unit='ms')
        return dataFrame

def prisma_extract_config_rql_search_timerange(query,timerange):

    payload= {
        "skipSearchCreation": "true",
        "limit": 0,
        "withResourceJson": "false",
        "skipResult": "false",
        "sort": [
            {
                "field": "id",
                "direction": "asc"
            }
        ]
    }
    logger.info(
                f"timernage: {timerange}"
            )
    logger.info(
                f"query: {query}"
            )
    payload.update({"timeRange":timerange})
    payload.update({"query": query})
    
    PRISMA_TOKEN = generate_prisma_token(ACCESS_KEY, SECRET_KEY, CSPM_ENDPOINT)
    rql_search_result=list()
    NEXT_PAGE = ""
    sleep_counter=0
    ag = fetch_account_group_data(PRISMA_TOKEN, CSPM_ENDPOINT)
    while True:
        if NEXT_PAGE:
            payload.update({"nextPageToken": NEXT_PAGE})

        response, status_code = prisma_get_config_rql_search(
                    PRISMA_TOKEN, CSPM_ENDPOINT, payload
            )
        if status_code == 504:
            logger.info(
                f"API returned response: {status_code} - Sleeping 5..."
            )
            sleep_counter+=1
            sleep(SLEEP_AMOUNT)
        if sleep_counter > 5:
                logger.error(
                    f"Unable to grab compliance posture."
                )
                break
        elif status_code == 200:
            for item in response["items"]:
                for group in ag:
                    if item['accountId'] in group['accountIds']:
                        if 'accountGroup' not in item:
                            item['accountGroup'] = []
                            item['accountGroup'].append(group['name'])
                        else:
                            item['accountGroup'].append(group['name'])
                rql_search_result.append(item)

            if "nextPageToken" in response:
                NEXT_PAGE = response["nextPageToken"]
                logger.info(f"Getting next page of response, {NEXT_PAGE}")
            else:
                break
        elif status_code == 401:
            logger.info(
                f"API returned response: {status_code} - generating new Prisma token..."
            )

            PRISMA_TOKEN = generate_prisma_token(
                ACCESS_KEY, SECRET_KEY, CSPM_ENDPOINT
            )
        elif status_code == 500:
            logger.error(
                f"API returned Internal Server Error"
            )

            break
        elif status_code == 503:
            logger.info(
                f"API returned response: {status_code} - Sleeping 5..."
            )
            sleep_counter+=1
            sleep(SLEEP_AMOUNT)
        elif status_code == 400:
            logger.info(
                f"API returned response: {status_code} - Sleeping 5..."
            )
            sleep_counter+=1
            sleep(SLEEP_AMOUNT)
        else:
            logger.error(
                f"API returned response: {status_code}..."
            )
    return rql_search_result

def prisma_extract_config_rql_search(query):

    payload= {
        "skipSearchCreation": "true",
        "limit": 0,
        "withResourceJson": "false",
        "timeRange": {
            "type": "to_now",
            "value": "epoch"
        },
        "skipResult": "false",
        "sort": [
            {
                "field": "id",
                "direction": "asc"
            }
        ]
    }
    payload.update({"query": query})
    
    PRISMA_TOKEN = generate_prisma_token(ACCESS_KEY, SECRET_KEY, CSPM_ENDPOINT)
    rql_search_result=list()
    NEXT_PAGE = ""
    sleep_counter=0
    ag = fetch_account_group_data(PRISMA_TOKEN, CSPM_ENDPOINT)
    while True:
        if NEXT_PAGE:
            payload.update({"nextPageToken": NEXT_PAGE})

        response, status_code = prisma_get_config_rql_search(
                    PRISMA_TOKEN, CSPM_ENDPOINT, payload
            )
        if status_code == 504:
            logger.info(
                f"API returned response: {status_code} - Sleeping 5..."
            )
            sleep_counter+=1
            sleep(SLEEP_AMOUNT)
        if sleep_counter > 5:
                logger.error(
                    f"Unable to grab compliance posture."
                )
                break
        elif status_code == 200:
        
            for item in response["items"]:
                for group in ag:
                    if item['accountId'] in group['accountIds']:
                        if 'accountGroup' not in item:
                            item['accountGroup'] = []
                            item['accountGroup'].append(group['name'])
                        else:
                            item['accountGroup'].append(group['name'])
                rql_search_result.append(item)

            if "nextPageToken" in response:
                NEXT_PAGE = response["nextPageToken"]
                logger.info(f"Getting next page of response, {NEXT_PAGE}")
            else:
                break
        elif status_code == 401:
            logger.info(
                f"API returned response: {status_code} - generating new Prisma token..."
            )

            PRISMA_TOKEN = generate_prisma_token(
                ACCESS_KEY, SECRET_KEY, CSPM_ENDPOINT
            )
        elif status_code == 500:
            logger.error(
                f"API returned Internal Server Error"
            )

            break
        elif status_code == 503:
            logger.info(
                f"API returned response: {status_code} - Sleeping 5..."
            )
            sleep(SLEEP_AMOUNT)
        else:
            logger.error(
                f"API returned response: {status_code}..."
            )
    return rql_search_result

def prisma_get_rql_query_to_dataframe(query,startdate,end_date):
        dataFrame=pd.DataFrame()
        while(end_date >= startdate):
            enddate = startdate + datetime.timedelta(days=1) -datetime.timedelta(seconds=1)
            timeRange =  {
                "type": "absolute",
                "value": { "startTime": calendar.timegm(startdate.timetuple()) * 1000,
                        "endTime": calendar.timegm(enddate.timetuple()) * 1000
                }
            }
          
            rql_search_data = prisma_extract_config_rql_search_timerange(query,timeRange)
            tempDataFrame=pd.DataFrame(rql_search_data)
            tempDataFrame['startDate'] = startdate
            tempDataFrame['endDate'] = enddate
            dataFrame=pd.concat([dataFrame, tempDataFrame])

            startdate= startdate + datetime.timedelta(days=1) 

        dataFrame['insertTs'] = pd.to_datetime(dataFrame['insertTs'] , unit='ms')
        dataFrame['createdTs'] = pd.to_datetime(dataFrame['createdTs'] , unit='ms')
        if "addColumn mfa_active" in query:
            dataFrame['Passed'] = pd.json_normalize(dataFrame.dynamicData)['mfa_active']
        if "addColumn encrypted" in query:
            dataFrame['Passed'] = pd.json_normalize(dataFrame.dynamicData)['encrypted']
        return dataFrame
