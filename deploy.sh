#!/bin/bash

deploy/cloudfunction.sh &
P1=$!
deploy/cloudrun.sh &
P2=$!
deploy/dashapp.sh &
P3=$!
wait $P1 $P2 $P3