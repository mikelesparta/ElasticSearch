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

def exercise2():

    PASSWORD = ""

    # Create client and connect it to the server
    global es

    es = Elasticsearch(
        "https://localhost:9200",
        ca_certs="./http_ca.crt",
        basic_auth=("elastic", PASSWORD)
    )

    # Ask the user for the topic they want to search
    topicToSearch = input(prompt="What topic do you want to search? ")

    #Produce a dump with all the tweets related to a given topic
    results = helpers.scan(es,
        index="tweets-20090624-20090626-en_es-10percent",
        query={
            "query": {
                # The query for an input topic
                "query_string": {
                    "query": topicToSearch
                }
            }
        }
    )

    f=open("scan-dump-ex2.txt","wb")

    # Iteramos sobre los resultados, no es preciso preocuparse de las
    # conexiones consecutivas que hay que hacer con el servidor ES
    for hit in results:
        text = hit["_source"]["text"]

        # Para visualizar mejor los tuits se sustituyen los saltos de línea
        # por espacios en blanco *y* se añade un salto de línea tras cada tuit
        text = text.replace("\n"," ")+"\n"
        f.write(text.encode("UTF-8"))

    f.close()

    print("Output file scan-dump-ex2.txt generated")

    #  Search 5 significant terms
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "query":topicToSearch
                }
            },
            "aggs": {
                "Significant terms":{
                    "significant_terms":{
                        "field":"text",
                        "size":5,
                        "gnd":{}
                    }
                }
            }
        },
        request_timeout=30
    )

    # First query significant terms need to be used in the expansion for the
    # second query
    # Contains the 5 signifiant terms from the topic given
    sigTerms = ""

    # Print results
    print("Significnat terms for", topicToSearch)

    for sigTerm in results["aggregations"]["Significant terms"]["buckets"]:
        # Get the key of the tweet
        key = sigTerm["key"]

        # Add the significant term
        sigTerms += key + " "
        print("\t",key)

    # Since the initial query needs to be expanded
    # Second query with new significant terms added from the initial query
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "query":sigTerms
                }
            },
            "aggs": {
                "Significant terms":{
                    "significant_terms":{
                        "field":"text",
                        "size":8,
                        "gnd":{}
                    }
                }
            }
        }
    )

    print("\nFINAL SIGNIFICANT TERMS")

    for sigTerm in results["aggregations"]["Significant terms"]["buckets"]:
        # Get the key of the tweet
        key = sigTerm["key"]

        # Add the significant term
        print("\t",key)


if __name__ == '__main__':
    exercise2()