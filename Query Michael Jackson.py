#-------------------------------------------------------------------------------
# Name:        Ejercicio Michael Jackson
# Purpose:
#
# Author:      mikel
#
# Created:     29/11/2022
# Copyright:   (c) mikel 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import json # Para poder trabajar con objetos JSON

from elasticsearch import Elasticsearch
from elasticsearch import helpers

import json # Para poder trabajar con objetos JSON

def main():
    # Password para el usuario 'lectura' asignada por nosotros
    PASSWORD = ""

    # Creamos el cliente y lo conectamos a nuestro servidor
    global es

    es = Elasticsearch(
        "https://localhost:9200",
        ca_certs="./http_ca.crt",
        basic_auth=("elastic", PASSWORD)
    )


##    results = es.search(
##        index="tweets-20090624-20090626-en_es-10percent",
##        body={
##            "size": 10000,
##            "from": 10000,
##            "query": {
##                "match": {
##                    "text":"michael jackson"
##                }
##            }
##        }
##    )
##
##    print(str(results["hits"]["total"]) + " resultados para la query q=\"michael jackson\"")
##    for i, hit in enumerate(results['hits']['hits']):
##        print(i, hit['_source']['text'])


    # Using pagination and sorting
##    {
##        "size": 10000,
##        "from": 0,
##        "query": {
##            "match": {
##                "text":"michael jackson"
##            }
##        },
##        "search_after": ["3339665684"],
##        "sort": {
##            "id_str": "asc"
##        }
##    }

    # Using helpers.scan, need to import helpers
    iterator = helpers.scan(es,
        index="tweets-20090624-20090626-en_es-10percent",
        query = {
            "query": {
                "match": {
                    "text":"michael jackson"
                }
            }
        }
    )

    for i, doc in enumerate(iterator):
        print(i, doc['_source']['text'])


if __name__ == '__main__':
    main()
