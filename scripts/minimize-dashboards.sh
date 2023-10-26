#!/bin/bash

# You will need to install jq to use this tool.
# brew install jq

set -e

DIR="etc/grafana/dashboards"

for dashboard in "${DIR}"/*.json; do
    if [[ "$dashboard" == *.min.json ]]; then
        continue
    fi
    name=$(basename "$dashboard" .json)
    jq -c < "$dashboard" > "${DIR}/${name}.min.json"
done

for dashboard in "${DIR}"/*.min.json; do
    name=$(basename "$dashboard" .min.json)
    mv "$dashboard" "${DIR}/${name}.json"
done

if [ "$1" == "--check" ] ; then
 if ! git diff --exit-code; then
    echo "Please run minimize-dashboards.sh and commit after editing the grafana dashboards."
    exit 1
 fi
fi