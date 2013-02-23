#!/bin/bash

set -u

if [ $# -ne 1 ]; then
    echo "Usage: "$0" [output_dir]"
    exit
fi

output_dir=$1

cd $output_dir

graph_lines=$(ls trial1/*.graph | awk -F".graph" '{ print $1 }' | awk -F"trial1/" '{ print $2 }')

for graph_line in $graph_lines; do
    all_file=${graph_line}".all"
    graph_file=${graph_line}".graph"

    echo -n > $all_file
    echo -n > $graph_file

    for data_point in $(ls trial1/$graph_line.*.tput | awk -F"trial1/" '{ print $2 }' ); do
	control_var=$(echo $data_point | awk -F"." '{ print $2 }')
	all_tputs=""


	for trial in $(ls -d trial*); do
            tput=$(cat ${trial}/${data_point})
            all_tputs=$(echo -e "${all_tputs}\t${tput}")
	done
	#sort all tputs
	all_tputs=$(echo $all_tputs | sed 's/ /\n/g' | sort -n | xargs)
	num_tputs=$(echo $all_tputs | wc -w)
	median_tput_line=$(echo "(${num_tputs}+1)/2" | bc)
	median_tput=$(echo $all_tputs | awk '{ print $'$median_tput_line' }')

	echo -e "${control_var}${all_tputs}" >> ${all_file}
	echo -e "${control_var}\t${median_tput}" >> ${graph_file}
    done

    cat $all_file | sort -n > tmp
    mv tmp $all_file

    cat $graph_file | sort -n > tmp
    mv tmp $graph_file
done
