#!/bin/bash

matches=$(egrep "ERROR|assert" cassandra_system*log)

if [ "$matches" == "" ]; then
    exit 0
else
    echo "$matches" | head -n 5
    exit 1
fi