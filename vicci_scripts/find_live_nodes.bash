#!/bin/bash

set -u

echo -n "" > live_nodes
failures=""
for school in princeton washington stanford gt; do
    for i in $(seq 70); do
	host="node${i}.${school}.vicci.org"
	echo -n ${host}": "
	ping -c 1 -w 3 ${host} > /dev/null 2>&1
	ping_ret=$?
	if [[ ${ping_ret} -eq 0 ]]; then
	    echo "success"
	    echo ${host} >> live_nodes
	else
	    echo "failed, ping return $ping_ret"
	    failures=$(echo -e "${failures}\n${host}")
	fi
    done
done

echo -n "Failures:"
echo "${failures}"