#!/bin/bash

set -u

if [ $# -ne 1 ]; then
    echo "Usage: "$0" [output_dir]"
    exit
fi

output_dir=$1


for system in cops2 vanilla; do
    per_cli_file=$output_dir/$system".per_client_tput"

    #datapoints=$(ls $output_dir/trial1/cops2/client0/*+tput | awk -F"+" '{ print $2 }' | sort -un)
    datapoints=$(ls $output_dir/trial1/*/client0/*+tput | awk -F"+" '{ print $2 }' | sort -un)

    echo -n "" > $per_cli_file
    for dp in $datapoints; do 
	echo -e "---- $dp ----" >> $per_cli_file
	echo -e "trial\tmin\tmedian\tmax\tmean±stddev" >> $per_cli_file
	for tri_dir in $(ls -d $output_dir/trial*); do 
	    awk '{ print $1 }' $tri_dir/$system/client*/*+$dp+tput | sort -n > sorted_ops_tmp
	    min=$(head -n 1 sorted_ops_tmp)
	    med=$(head -n $(( ($(cat sorted_ops_tmp | wc -l) +1)/ 2)) sorted_ops_tmp | tail -n 1)
	    max=$(tail -n 1 sorted_ops_tmp)
	    mean=$(cat sorted_ops_tmp | awk '{ sum+=$1 } END {printf("%d",sum/NR)}')
	    stddev=$(cat sorted_ops_tmp | awk '{sum+=$1; array[NR]=$1} END {for(x=1;x<=NR;x++){sumsq+=((array[x]-(sum/NR))*(array[x]-(sum/NR)));} printf("%d", sqrt(sumsq/NR))}')

	    tri=$(echo $tri_dir | awk -F"trial" '{ print $2 }')
	    echo -e "$tri\t$min\t$med\t$max\t$mean±$stddev" >> $per_cli_file
	    echo -e "all\t$(cat sorted_ops_tmp | xargs)" >> $per_cli_file
	    rm sorted_ops_tmp
	done
	echo >> $per_cli_file
    done
done
