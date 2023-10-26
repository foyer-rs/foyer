#!/bin/bash

# You will need to install jq to use this tool.
# brew install jq

set -e

# DIR="etc/grafana/dashboards/"
# DASHBOARDS="etc/grafana/dashboards/*.json"

DIR="etc/grafana/dashboards/"


for dashboard in `ls $DIR | grep -v '.min.json'`; do
    name=`basename $dashboard .json`
    cat "${DIR}${dashboard}" | jq -c > "${DIR}${name}.min.json"
done

if [ "$1" == "--check" ] ; then
 if ! git diff --exit-code; then
    echo "Please run minimize-dashboards.sh and commit after editing the grafana dashboards."
    exit 1
 fi
fi