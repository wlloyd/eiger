#!/bin/bash

set -u

if [ $# -ne 1 ]; then
    echo "$0: [stress pid]"
    exit 1
fi

stress_pid=$1

group_id=$(ps x -o "pid, pgid" | grep $stress_pid | awk '{ print $2 }')
stress_ids=$(ps x -o "pid, ppid, command" | grep $group_id | grep stress | awk '{ print $1 }')
echo $stress_ids
for pid in $stress_ids; do
    kill $pid
done