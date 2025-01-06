import argparse
from helpers import *

SLEEP_AMOUNT=5


def generate_kri_data_file(config, date,end_date):
    dataFrame=pd.DataFrame()

    queries=config['queries']
    
    for key in queries:
        df = prisma_get_rql_query_to_dataframe(queries[key],date, end_date)
        df['sheet_name']=key
        dataFrame = pd.concat([dataFrame, df.loc[:, ['rrn', 'stateId', 'assetId', 'id', 'name', 'accountId', 'accountName',
                                                        'cloudType', 'regionId', 'regionName', 'service', 'resourceType',
                                                        'Passed', 'startDate', 'endDate', 'sheet_name', 'accountGroup']]])
    

    compared_queries=config['compared_queries']

    for key in compared_queries:
        df = prisma_get_compared_rql_queries_dated(compared_queries[key][0], compared_queries[key][1],date, end_date)
        df['sheet_name']=key
        dataFrame = pd.concat([dataFrame, df.loc[:, ['rrn', 'stateId', 'assetId', 'id', 'name', 'accountId', 'accountName',
                                                        'cloudType', 'regionId', 'regionName', 'service', 'resourceType',
                                                        'Passed', 'startDate', 'endDate', 'sheet_name', 'accountGroup']]])


    dataFrame.reset_index(drop=True, inplace=True)
    if dataFrame.empty:
        logging.warning("DataFrame is empty")
    else:
        write_dataframe_to_sql(dataFrame)



def main(event="", context=""):
    config={
        "queries": {
             "AWS MFA is not enabled on Root account":"config from cloud.resource where cloud.type = 'aws'  AND api.name = 'aws-iam-get-credential-report' AND json.rule = user equals \"<root_account>\" AND arn does not contain \"gov:\" addColumn mfa_active" ,
             "EBS Volumes should be encrypted": "config from cloud.resource where cloud.type = 'aws'  AND api.name = 'aws-ec2-describe-volumes' addColumn encrypted" 
#             "AWS_root_account_configured_with_Virtual_MFA_failed_dated": "config from cloud.resource where cloud.type = 'aws' AND api.name = 'aws-iam-list-virtual-mfa-devices' AND json.rule = 'serialNumber contains root-account-mfa-device and user.arn contains root'" 
        },
        "compared_queries" : {
            "AWS Default Security Group does not restrict all traffic" : 
                ["config from cloud.resource where cloud.type = 'aws'  AND api.name = 'aws-ec2-describe-security-groups' AND json.rule = '((groupName == default) and (ipPermissions[*] is not empty or ipPermissionsEgress[*] is not empty))'" ,  "config from cloud.resource where cloud.type = 'aws'  AND api.name = 'aws-ec2-describe-security-groups'" ],
            "AWS EC2 instance not configured with Instance Metadata Service v2 (IMDSv2)" : 
                ["config from cloud.resource where cloud.type = 'aws' AND api.name = 'aws-ec2-describe-instances' AND json.rule = state contains running and metadataOptions.httpEndpoint equals enabled and metadataOptions.httpTokens does not contain required", "config from cloud.resource where cloud.type = 'aws' AND api.name = 'aws-ec2-describe-instances' AND json.rule = state contains running"],
            "AWS MFA not enabled for IAM users" :
                ["config from cloud.resource where cloud.type = 'aws' and api.name='aws-iam-get-credential-report' AND json.rule='password_enabled equals true and mfa_active is false'", "config from cloud.resource where cloud.type = 'aws' and api.name='aws-iam-get-credential-report' AND json.rule='password_enabled equals true and mfa_active is true'"],
            "Wide Open Security Group Ingress allowed from All IP" :
                ["config from cloud.resource where cloud.type = 'aws' AND api.name = 'aws-ec2-describe-security-groups' AND json.rule = ipPermissions[].ipv4Ranges[].cidrIp contains \"0.0.0.0/0\"", "config from cloud.resource where cloud.type = 'aws' AND api.name = 'aws-ec2-describe-security-groups' AND json.rule = ipPermissions[].ipv4Ranges[].cidrIp does not contain \"0.0.0.0/0\""]
        }
    }
    start_time = dt.datetime.now()

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start_date", dest="start_date", nargs='?', const=datetime.datetime(2024,1,1,0,00), type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), help="Date in the format yyyy-mm-dd")
    parser.add_argument("-e", "--end_date", dest="end_date", nargs='?', const=datetime.datetime(2024,1,1,0,00), type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), help="Date in the format yyyy-mm-dd")
    parser.add_argument("-d", "--daily", action='store_true' , help="Inserts into datasource information from yesterdays date")
    parser.add_argument("-i", "--initialize", action='store_true' , help="Initializes db table")
    parser.add_argument("-b", "--between", dest="between", nargs=2 )
    parser.add_argument("-t", "--test", action='store_true' , help="Tests connection to database")
    args=parser.parse_args()

    if args.start_date is not None:
        logging.info("StartDate :" + args.start_date.strftime('%Y-%m-%d'))
        enddate=""
        if args.end_date is not None:
            enddate=args.end_date 
        else:
            enddate=datetime.datetime.now() - datetime.timedelta(days=1)
        logging.info("EndDate :" + args.end_date.strftime('%Y-%m-%d'))
        generate_kri_data_file(config,args.start_date, enddate)
    elif args.daily:
        date=datetime.date.today() - datetime.timedelta(days=1)
        date=datetime.datetime(date.year, date.month, date.day)
        logging.info(date)
        date=datetime.datetime(date.year, date.month, date.day)
        today = datetime.datetime.now()
        generate_kri_data_file(config,date,today)
    elif args.initialize:
        initialize_zoom_tables()
    elif args.execute is not None:
        run_sql_command(args.execute)
    elif args.test:
        test_zoom_tables()

    end_time = dt.datetime.now()
    time_diff = end_time - start_time

    logger.info(f"Script finished, total time taken: {time_diff}")

    return

if __name__ == "__main__":
    main()
