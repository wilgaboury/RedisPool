#!/bin/bash

ports=(7000 7001 7002)
for port in "${ports[@]}"; do
    sudo kill $(sudo lsof -t -i:$port)
done