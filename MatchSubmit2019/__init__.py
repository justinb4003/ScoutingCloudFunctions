import os
import uuid
import logging
import azure.functions as func

from azure.cosmos import exceptions, CosmosClient, PartitionKey


def main(req: func.HttpRequest) -> func.HttpResponse:
    # NOTE: If you don't see the first logging statement here when debugging
    # in Azure then the import statements above are likely the problem.
    # If you add a new import that you installed via pip locally be sure to
    # add it to requirements.txt.
    logging.info('MatchSubmit2019 Begins')
    cosmos_uri = os.environ['CosmosURI']
    cosmos_key = os.environ['CosmosKey']

    client = CosmosClient(cosmos_uri, cosmos_key)
    database_name = 'TrisonicsScouting'
    database = client.create_database_if_not_exists(id=database_name)
    container_name = 'MatchResults2019'
    container = database.create_container_if_not_exists(
        id=container_name, 
        partition_key=PartitionKey(path="/eventKey"),
        offer_throughput=400)

    error_found = False
    error_msg = ''
    try:
        # A ValueError will get tossed by req.get_json() if there's no body.
        req_body = req.get_json()

        # Now enforce some sanity check(s) on the document before we accept it
        if not 'eventKey' in req_body.keys():
            error_found = True
            new_msg = 'No eventKey element defined in JSON. '
            error_msg += new_msg
            logging.error(new_msg)
        
        req_body['id'] = str(uuid.uuid4())

        if error_found is False:
            container.create_item(body=req_body)
    except ValueError:
        error_found = True
        error_msg = 'No JSON body found'
        logging.error(error_msg)

    if error_found:
        return func.HttpResponse(error_msg, status_code=400)
    else:
        return func.HttpResponse(f'Document accepted')