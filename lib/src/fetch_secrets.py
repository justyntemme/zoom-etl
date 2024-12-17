import json
import boto3
import boto3.session
import os

#from boto3 import exceptions
from botocore.exceptions import ClientError

#AWS_BUCKET=os.getenv("AWS_BUCKET")
AWS_REGION= os.getenv("AWS_REGION")

def fetch_secret(secret_name: str):
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
    
    return get_secret_value_response['SecretString']

PRISMA_API_KEY = json.loads(fetch_secret("PRISMA_KEY"))
os.environ['PRISMA_ACCESS_KEY'] = PRISMA_API_KEY['AccessKey']
os.environ['SECRET_KEY'] = PRISMA_API_KEY['SecretKey']

AWS_DB_SECRET = json.loads(fetch_secret("AWS_DB_SECRET_KEY"))
os.environ['AWS_DB_USER'] = AWS_DB_SECRET['username']
os.environ['AWS_DATABASE_PASSWORD'] = AWS_DB_SECRET['password']
os.environ['AWS_DB_PORT'] =  AWS_DB_SECRET['port']
os.environ['AWS_DATABASE'] = AWS_DB_SECRET['host']

#AWS_DB_NAME = os.getenv("AWS_DB_NAME")
#AWS_DB_SECRET_KEY = os.getenv("AWS_DB_SECRET_KEY")

#def get_aws_secret(secret_name):
    #session = boto3.session.Session()
    #client = session.client(
        #service_name='secretsmanager',
        #region_name=AWS_REGION
    #)

    #try:
        #get_secret_value_response = client.get_secret_value(
            #SecretId=secret_name
        #)
    #except ClientError as e:
        #raise e

    #secret = get_secret_value_response['SecretString']

    #return secret