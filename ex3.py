#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      mikel
#
# Created:     13/12/2022
# Copyright:   (c) mikel 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import json # To work with JSON objects
import requests # To work with requests to web pages

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from datetime import datetime

def exercise3():

    PASSWORD = ""

    # Create client and connect it to the server
    global es

    es = Elasticsearch(
        "https://localhost:9200",
        ca_certs="./http_ca.crt",
        basic_auth=("elastic", PASSWORD)
    )

    # Search 5 significant terms
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
        # Body with More like this query, looking for a query
        # which is related to iran on the text field
        body = {
            "size": 5,
            "_source": ["title", "description"],
            "query": {
                "more_like_this": {
                    "fields": ["text"],
                    "like": ["iran"],
                    "max_query_terms": 10,
                    "min_term_freq": 1
                }
            }
        },
        request_timeout=30
    )

    print("Results:\n", results)

    # Print Ids retrieved
    print("\nIDs:")

    for res in results["hits"]["hits"]:
        id = res["_id"]
        print("\t",id)


if __name__ == '__main__':
    exercise3()