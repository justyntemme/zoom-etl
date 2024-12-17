import pandas as pd
from . import data_export as dataexport
from logging import Logger

# Perform the work here of using the other files (db, data_export, prisma_cloud_api, etc.)
# main.py should just call into here and we propagate the work


def print_queries(data):
    print("\nQUERIES:")
    if data.get("config") is None or data.get("config").get("queries") is None:
        return

    for query in data["config"]["queries"]:
        print(f"{query['name']}:\n\t- {query['query']}")


def print_compared_queries(data):
    if data.get("config") is None or data.get("config").get("compared_queries") is None:
        return

    print("\nCOMPARED_QUERIES:")
    for compared in data["config"]["compared_queries"]:
        print(f"\n{compared['name']}:")

        for query in compared["queries"]:
            print(f"\t- {query}")


def generate_kri_data_file(
    logger: Logger, config, start_date, end_date
) -> pd.DataFrame:
    data_frame = pd.DataFrame()

    # queries=config['queries']

    for query, query_data in config.items():  # data['config']['queries']:
        # for key in query_data: #queries:
        # df = prisma_get_rql_query_to_dataframe(queries[key],date, end_date)
        temp_frame = dataexport.query_data_to_dataframe(
            logger, query_data, start_date, end_date
        )
        temp_frame["sheet_name"] = query
        data_frame = pd.concat(
            [
                data_frame,
                temp_frame.loc[
                    :,
                    [
                        "rrn",
                        "stateId",
                        "assetId",
                        "id",
                        "name",
                        "accountId",
                        "accountName",
                        "cloudType",
                        "regionId",
                        "regionName",
                        "service",
                        "resourceType",
                        #'Passed',
                        "startDate",
                        "endDate",
                        "sheet_name",
                    ],
                ],
            ]
        )

        return data_frame

    # compared_queries=config['compared_queries']

    # for key in compared_queries:
    #    df = prisma_get_compared_rql_queries_dated(compared_queries[key][0], compared_queries[key][1],start_date, end_date)
    #    df['sheet_name']=key
    #    dataFrame = pd.concat([dataFrame, df.loc[:, ['rrn', 'stateId', 'assetId', 'id', 'name', 'accountId', 'accountName',
    #                                                    'cloudType', 'regionId', 'regionName', 'service', 'resourceType',
    #                                                    'Passed', 'startDate', 'endDate', 'sheet_name']]])

    # dataFrame.reset_index(drop=True, inplace=True)
    # if dataFrame.empty:
    #    logging.warning("DataFrame is empty")
    # else:
    #    write_dataframe_to_sql(dataFrame)
