import pandas as pd
import calendar
import logging as Logger
from . import prisma_cloud_api as pcapi

def query_data_to_dataframe(logger: Logger, query: dict, start_date = None, end_date = None) -> pd.DataFrame:
    data_frame = pd.DataFrame()
    time_range = {}

    if start_date and end_date:
        time_range =  {
            'timerange': {
                "type": "absolute",
                "value": { 
                    "startTime": calendar.timegm(start_date.timetuple()) * 1000,
                    "endTime": calendar.timegm(end_date.timetuple()) * 1000
                }
            }
        }

    rql_search_data = pcapi.work(logger, query, time_range)

    for response in rql_search_data:
        for response_item in response['data']['items']:
            temp_frame=pd.DataFrame(response_item)
            data_frame = pd.concat([data_frame, temp_frame])

    if start_date and end_date:
        data_frame['startDate'] = start_date
        data_frame['endDate'] = end_date
    data_frame['insertTs'] = pd.to_datetime(data_frame['insertTs'] , unit='ms')
    data_frame['createdTs'] = pd.to_datetime(data_frame['createdTs'] , unit='ms')

    if "addColumn mfa_active" in query:
        data_frame['Passed'] = pd.json_normalize(data_frame.dynamicData)['mfa_active']

    if "addColumn encrypted" in query:
        data_frame['Passed'] = pd.json_normalize(data_frame.dynamicData)['encrypted']

    return data_frame
