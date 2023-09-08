#!/bin/bash

compose=$(docker compose ps -q | wc -l)

cat <<EOF > docker-compose.override.yaml
version: '3'

services:
    grafana:
        user: "${UID}"
EOF

if [ "$compose" = 0 ] ; then
    docker compose up -d
else
    docker compose down
fi