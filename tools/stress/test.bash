#!/bin/bash

set -u

op=insert
#op=read

#for col_and_size in 1:1 10:1 100:1 1000:1; do
for col_and_size in 1:1 10:1 100:1 1000:1 1:100 10:100 100:100 1000:100 1:1000 10:1000 100:1000 1:10000; do

    columns=$(echo $col_and_size | awk -F":" '{ print $1 }')
    size=$(echo $col_and_size | awk -F":" '{ print $2 }')
    tot_size=$((columns * size))

    num_keys=2000000
    if [[ $tot_size -gt 9999 ]]; then
	num_keys=200000
    fi
    if [[ $tot_size -gt 99999 ]]; then
	num_keys=20000
    fi

    set -x
    bin/stress --num-different-keys=1000 --columns=$columns --column-size=$size -d node5.princeton.vicci.org --num-keys=$num_keys -o $op
    set +x
done