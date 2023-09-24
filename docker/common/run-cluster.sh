#!/bin/bash

launch_redis_and_wait()
{
    sed "s/<PORT>/$2/g" redis-cluster.conf.template >> $1/redis.conf
    cd $1
    output=$(mktemp "${TMPDIR:-/tmp/}$(basename $0).XXX")
    redis-server ./redis.conf &> $output &
    until grep -q -i "Ready to accept connections tcp" $output
    do
        sleep 0.5
    done
    cd ..
}

ADDRS=()

for i in $(seq 0 2); do
    NODE_NUM=$(expr $i + 1)
    NODE_NAME="node${NODE_NUM}"
    NODE_PORT=%(expr 7000 + $i)
    mkdir $NODE_NAME
    launch_redis_and_wait $NODE_NAME $NODE_PORT
    ADDRS+="127.0.0.1:${NODE_PORT}"
done

redis-cli --cluster create ${ADDRS[@]} --cluster-replicas 0 --cluster-yes