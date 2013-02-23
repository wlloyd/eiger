#!/bin/bash

#run with:
# for n in 2 4 8 16 32 64 128; do ./make_kodiak_cfg.bash $n; done

set -u

bad_node_list="h46 h47 h51"
bad_node_list=${bad_node_list}" " #trailing space required

if [ $# -ne 1 ]; then
    echo "$0: [total server count]"
    exit
fi

num_srvs=$1

srv_file=kodiak_${num_srvs}
cli_file=kodiak_${num_srvs}_clients

echo "num_dcs=2" > $srv_file
echo "num_dcs=2" > $cli_file

h_index=-1
for i in $(seq $num_srvs ); do
    ((h_index++))
    while [ "$(echo "$bad_node_list" | grep h${h_index}'[[:space:]]')" != "" ]; do
	((h_index++))
    done

    echo "cassandra_ips=h"${h_index}".ib0" >> $srv_file
done

for i in $(seq $num_srvs); do
    ((h_index++))
    while [ "$(echo "$bad_node_list" | grep h${h_index}'[[:space:]]')" != "" ]; do
	((h_index++))
    done

    echo "cassandra_ips=h"${h_index}".ib0" >> $cli_file
done