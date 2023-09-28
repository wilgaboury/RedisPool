#!/bin/bash

./run-cluster.sh

envoy -c config.yaml &

echo CLUSTER READY!

sleep infinity