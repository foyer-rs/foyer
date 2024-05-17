#!/bin/bash

compose=$(docker compose ps -q | wc -l)

cat <<EOF > docker-compose.override.yaml
services:
    prometheus:
        user: "${UID}"
EOF

if [ "$compose" = 0 ] ; then
    mkdir -p .tmp/prometheus
    docker compose up -d
else
    docker compose down
fi