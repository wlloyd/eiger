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

#kill in parallel
set -m #need monitor mode to fg processes
for ip in ${ips[@]}; do
    ssh -t -t -o StrictHostKeyChecking=no princeton_cops@$ip "/home/princeton_cops/cops2/kill_all_cassandra.bash" &
done

for ip in ${ips[@]}; do
    fg
done