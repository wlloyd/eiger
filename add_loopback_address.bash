#!/bin/bash
#
# Add up to N loopback addresses
#
# Tested on Mac OS 10.7.2

set -u

if [ $# -ne 1 ]; then
    echo "Usage: "$0" [number of loopback addresses to add (starting at 127.0.0.2)]"
    exit
fi

num_addrs=$1

if [[ $num_addrs -gt 254 || $num_addrs -lt 1 ]]; then
    echo "Must be a small integer number of addresses, not ${num_addrs}"
    exit
fi

for i in $(seq 2 $((num_addrs + 1))); do
    sudo ifconfig lo0 inet 127.0.0.${i} add
done

#In case we're offline, we need this alias so getLocalHost() returns something to us
sudo ifconfig en0 alias 127.0.0.1

ifconfig