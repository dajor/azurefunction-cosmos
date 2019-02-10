
import logging
import azure.functions as func
import os, io
import pandas as pd

import json    

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors


config = {
    'ENDPOINT': os.environ['ENDPOINT'],
    'PRIMARYKEY': os.environ['PRIMARYKEY'],
    'DBLink': os.environ['DBLink']
}

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    file = myblob.read()
    logging.info (type(file))
    df = pd.read_csv(io.BytesIO(file), sep=';', dtype=str)
    logging.info (df)
    results = []
    results = json.loads(df.to_json(orient='records'))
    logging.info (len(results))
    client = cosmos_client.CosmosClient(url_connection=config['ENDPOINT'], auth={'masterKey': config['PRIMARYKEY']})
    
    for item in results:

        logging.info("Import")
        item['id'] = item['CONTRACT_ID']
        logging.info(json.dumps(item,indent=2))

        try:
            client.CreateItem(config['DBLink'], item)
        except errors.HTTPFailure as e:
            if e.status_code == 409:
                  query = {'query': 'SELECT * FROM c where c.id="%s"' % item['id']}
                  options = {}
                  
                  docs = client.QueryItems(config['DBLink'], query, options)
                  doc = list(docs)[0]

                  # Get the document link from attribute `_self`
                  doc_link = doc['_self']
                  client.ReplaceItem(doc_link, item)


        


    
