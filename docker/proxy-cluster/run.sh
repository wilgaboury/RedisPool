#!/bin/bash

./run-cluster.sh

envoy -c config.yaml &

sleep infinity