#!/bin/bash

subjects=$(curl localhost:8081/subjects | jq -r '.[]')

for subj in $subjects; do 
  curl -X DELETE "localhost:8081/subjects/$subj"
done;