#!/bin/bash
#
# Kills cassandra on all nodes mentioned in the dcl_config_file
#

set -u

if [ $# -ne 1 ]; then
    echo "Usage: "$0" [vicci_dcl_config_file]"
    exit
fi

dcl_config=$1

num_dcs=$(grep num_dcs $dcl_config | awk -F "=" '{ print $2 }')
ips=($(grep cassandra_ips $dcl_config | awk -F "=" '{ print $2 }'))
ips=($(echo "echo ${ips[@]}" | bash))
for ip in ${ips[@]}; do
    ssh -t -t -o StrictHostKeyChecking=no $ip "\
rm ~/cassandra_var/cassandra_system.*.log.*;\
rm -rf ~/cassandra_var/data/* 2> /dev/null;\
rm -rf ~/cassandra_var/commitlog/* 2> /dev/null;\
rm -rf ~/cassandra_var/saved_caches/* 2> /dev/null;\
rm -rf ~/cassandra_var/stdout/* 2> /dev/null;\
rm ~/cops2/*hprof;\
rm ~/cassandra-vanilla/*hprof;\
"
done