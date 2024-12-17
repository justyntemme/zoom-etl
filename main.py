import argparse
import logging
import datetime as dt
import lib.src.db as db
import lib.src.api as api
import lib.src.yaml as yaml

SLEEP_AMOUNT=5

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main(event="", context=""):
    start_time = dt.datetime.now()

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start_date", dest="start_date", nargs='?', const=dt.datetime(2024,1,1,0,00), type=lambda s: dt.datetime.strptime(s, '%Y-%m-%d'), help="Date in the format yyyy-mm-dd")
    parser.add_argument("-e", "--end_date", dest="end_date", nargs='?', const=dt.datetime(2024,1,1,0,00), type=lambda s: dt.datetime.strptime(s, '%Y-%m-%d'), help="Date in the format yyyy-mm-dd")
    parser.add_argument("-d", "--daily", action='store_true' , help="Inserts into datasource information from yesterdays date")
    parser.add_argument("-i", "--initialize", action='store_true' , help="Initializes db table")
    parser.add_argument("-b", "--between", dest="between", nargs=2 )
    parser.add_argument("-t", "--test", action='store_true' , help="Tests connection to database")
    # Mandatory
    parser.add_argument("files", type=str, help='yaml files or folders to be considered for querying data. can be absolute or relative paths')
    args=parser.parse_args()

    all_yaml_files = yaml.get_yaml_files(args.files)
    
    if len(all_yaml_files) == 0:
        logger.error("At least one YAML file is required. Please provide a file or directory containing at least one YAML file.")
        return
    
    yaml_data = yaml.parse_yaml_files(logger, all_yaml_files)

    for data in yaml_data.values():
        api.print_queries(data)
        api.print_compared_queries(data)

    if args.start_date is not None:
        logging.info("StartDate :" + args.start_date.strftime('%Y-%m-%d'))
        end_date=""
        if args.end_date is not None:
            end_date=args.end_date 
        else:
            end_date=dt.datetime.now() - dt.timedelta(days=1)
        logging.info("EndDate :" + args.end_date.strftime('%Y-%m-%d'))
 #       generate_kri_data_file(config, args.start_date, enddate)
        data_frame = api.generate_kri_data_file(logger, yaml_data, args.start_date, end_date)
        print(data_frame)
        data_frame.to_csv("frame_dump.csv")
    elif args.daily:
        date=dt.date.today() - dt.timedelta(days=1)
        date=dt.datetime(date.year, date.month, date.day)
        logging.info(date)
        date=dt.datetime(date.year, date.month, date.day)
        today = dt.datetime.now()
 #       generate_kri_data_file(config, date, today)
        data_frame = api.generate_kri_data_file(logger, yaml_data, date, today)

        print(data_frame)
        data_frame.to_csv("frame_dump.csv")
    elif args.initialize:
        db.initialize_zoom_tables(logger)
    #elif args.execute is not None:
        #db.run_sql_command(logger, args.execute)
    elif args.test:
        db.test_zoom_tables(logger)

    end_time = dt.datetime.now()

    time_diff = end_time - start_time

    #dataexport.query_data_to_dataframe(logger, yaml_data)#, date, today)

    #pcapi.work(logger, yaml_data)

    #session = auth()
    #validate_cspm_session(session)

    #for query_data in data['config']['queries']:
        #response = request_prisma_data(logger, session, query_data['query'], {})

        #print(response['data']['items'])
    

    logger.info(f"Script finished, total time taken: {time_diff}")

    return

if __name__ == "__main__":
    main()