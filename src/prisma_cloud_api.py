from logging import Logger
import os
from pcpi import session_loader, _cspm_session
from dotenv import load_dotenv

load_dotenv()

def auth() -> _cspm_session.CSPMSession:
    session_manager = session_loader.load_config_env(
        prisma_name="PRISMA_NAME",
        identifier_name="PRISMA_ACCESS_KEY", 
        secret_name="PRISMA_SECRET_KEY",
        api_url_name="CSPM_ENDPOINT",
        verify_name="PRISMA_VERIFY",
        project_flag_name="PRISMA_PROJECT_FLAG"
        #logger=py_logger
    )
    cspm_session = session_manager.create_cspm_session()

    return cspm_session

def validate_cspm_session(cspm_session: _cspm_session.CSPMSession) -> bool:
    res = cspm_session.request('GET', '/cloud')

    try:
        print(res.json())
    except Exception as err:
        print(f"CSPM Session Validation failed: {err}")

    return res is not None

def request_prisma_data(logger: Logger, cspm_session: _cspm_session.CSPMSession, query: str, params: dict):
    #endpoint = f"https://{cspm_endpoint}/search/api/v1/config"

    #logger.info("Gathering config rql search using endpoint: " + endpoint)

    #headers = {
    #    "accept": "application/json; charset=UTF-8",
    #    "content-type": "application/json; charset=UTF-8",
    #    "x-redlock-auth": token,
    #}

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
    #logger.info(
                #f"query: {query}"
            #)
    if params.get('timerange'):
        payload.update({"timeRange":params['timerange']})
        #logger.info(
                    #f"timerange: {params['timerange']}"
                #)
    else:
        payload.update({
            "timeRange": {
                "type": "to_now",
                "value": "epoch"
            }
        })
    payload.update({"query": query})

    return cspm_session.config_search_request(payload)

    #return cspm_session.request(
        #method='POST', 
        #endpoint_url="/search/api/v1/config", 
        #json=payload,
    #)

    #response = cspm_session.post(endpoint, headers=headers, json=payload)
    #/search/api/v1/config

def work(logger: Logger, data: dict, params: dict = {}):
    session = auth()
    validate_cspm_session(session)
    responses = []

    for file, query_data in data.items():
        logger.info(f"{file}:\n")

        for query in query_data['queries']:
            response = request_prisma_data(logger, session, query['query'], params)

            logger.info(f"Query[{query['name']}]: {response['data']['items']}")

            responses.append(response)
    
    return responses
