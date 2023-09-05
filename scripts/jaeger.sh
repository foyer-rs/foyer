#!/bin/bash

container="$(docker ps -a -q -f name=jaeger)"

if [ "$container" ] ; then
    echo "Stop jaeger..."
    docker stop "$container" > /dev/null
    docker rm "$container" > /dev/null
else
    echo "Start jaeger..."
    docker run -d --name jaeger \
        -p6831:6831/udp \
        -p6832:6832/udp \
        -p16686:16686 \
        -p4317:4317 \
        jaegertracing/all-in-one:latest \
        --collector.otlp.enabled=true \
        > /dev/null
    echo "Browser jaeger via http://localhost:16686"
fi