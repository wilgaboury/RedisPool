#!/bin/bash

cd "$(dirname "$0")"

cd ./common
docker build . --tag redis-common
cd ..

cd ./single
docker build . --tag redis-single
cd ..

cd ./cluster
docker build . --tag redis-cluster
cd ..

cd ./proxy-cluster
docker build . --tag redis-proxy-cluster

