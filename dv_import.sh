#!/bin/bash

# DataViper Batch Import
# 
# Imports line-delimited JSON objects into an ElasticSearch index.
# By default, the script will import all JSON files (*.json) in the current
# directory into an index named dataviper. Use command line arguments
# to override the default.
#
# For example, to import all Anti*.json JSON files into dataviper-antipublic:
# 
# dv_import.sh -i dataviper-antipublic -p Anti*.json
#
# Dependencies:
#   NodeJS
#   elasticdump
#

FILE_PATTERN=*.json
INDEX_NAME=dataviper
ES_URL=http://127.0.0.1:9200
BATCH_SIZE=1000
ED=~/.nvm/versions/node/v8.9.4/bin/elasticdump

usage()
{
    echo "usage: dv_import options"
    echo "  -i | --index index_name  Name of index to import into. Defaults to $INDEX_NAME"
    echo "  -p | --pattern pattern   File glob pattern to match. Defautls to $FILE_PATTERN"
    echo "  -s | --size batch_size   Number of objects to bulk create at time. Defaults to $BATCH_SIZE"
    echo "  -u | --url url           Elasticsearch URL. Defaults to $ES_URL"
    echo "  -h | --help              Print this message"
}

while [ "$1" != "" ]; do
    case $1 in
    -p | --pattern )        shift
                            FILE_PATTERN=$1
                            ;;

    -i | --index )          shift
                            INDEX_NAME=$1
                            ;;

    -s | --size )           shift
                            BATCH_SIZE=$1
                            ;;

    -u | --url )            shift
                            ES_URL=$1
                            ;;

    -h | --help )           usage
                            exit
                            ;;

    * )                     usage
                            exit 1

    esac
    shift
done

for f in $FILE_PATTERN
do
    echo "--------------------------------------------------------"
    echo "Importing $f into $ES_URL/$INDEX_NAME"
    echo "--------------------------------------------------------"
    elasticdump --input=$f --output=$ES_URL/$INDEX_NAME --ignore-errors --noRefresh --limit=$BATCH_SIZE
done

