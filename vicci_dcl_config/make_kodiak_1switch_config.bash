#!/bin/bash

#bad_nodes is egrep regex, e.g., "h68.ib0|h70.ib0"
bad_nodes="h68.ib0|h551.ib0|h612.ib0"

for n in 2 4 8 16 32 64 128 3 6 12 25 50 100 200; do
    echo "num_dcs=2" > kodiak_${n}A;
    cat /etc/hosts | grep "10.3.1" | sort -u | sort -n | awk '{ print "cassandra_ips="$2 }' | egrep -v $bad_nodes | head -n $n >> kodiak_${n}A
done

for n in 2 4 8 16 32 64 128 3 6 12 25 50 100 200; do 
    echo "num_dcs=2" > kodiak_${n}A_clients;
    #NOTE: we want to make sure client0 is client0 in all size configurations (the output collection scripts expect this)
    # If we don't have this, we'll end up counting the same client multiple times for smaller configurations
    # thus the sed script
    cat /etc/hosts | grep "10.3.1" | sort -u | sort -n | awk '{ print "cassandra_ips="$2 }' | egrep -v $bad_nodes | tail -n $n | sed '1!G;h;$!d' >> kodiak_${n}A_clients
done