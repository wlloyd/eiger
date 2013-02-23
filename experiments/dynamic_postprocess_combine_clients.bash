#!/bin/bash

set -u

if [ $# -ne 4 ]; then
    echo "Usage: "$0" [exp_dir] [output_dir] [run time] [trim]"
    exit
fi

exp_dir=$1
output_dir=$2
run_length=$3
trim=$4

for system_full in $(ls -dp ${output_dir}/* | grep '/$'); do
    system=$(echo $system_full | awk -F"${output_dir}/" '{ print $2 }' | sed 's/\///g')
    echo " $system"

    # postprocess all client dirs
    for client_full in $(ls -dp $system_full/* | grep '/$'); do
	client=$(echo $client_full | awk -F"${system_full}/" '{ print $2 }')
	echo -en "  Client Postprocess: $client    \r"

	${exp_dir}/dynamic_postprocess_client.bash $client_full $run_length $trim	
    done
    echo "  Client Postprocess: complete     "

    for data_point in $(ls $system_full/client0/*+tput | awk -F"client0/" '{ print $2 }' ); do
	control_var=$(echo $data_point | awk -F"+" '{ print $2 }')
	echo -en "  Combining Clients: $data_point      \r"


	all_tputs_ops=""
	all_tputs_keys=""
	all_tputs_cols=""
	all_tputs_bytes=""

	for client_full in $(ls -dp $system_full/* | grep '/$'); do
	    tput_ops=$(cat   ${client_full}/${data_point} 2>/dev/null | awk '{ print $1 }')
	    tput_keys=$(cat  ${client_full}/${data_point} 2>/dev/null | awk '{ print $2 }')
	    tput_cols=$(cat  ${client_full}/${data_point} 2>/dev/null | awk '{ print $3 }')
	    tput_bytes=$(cat ${client_full}/${data_point} 2>/dev/null | awk '{ print $4 }')
            all_tputs_ops=$(echo -e   "${all_tputs_ops}\t${tput_ops}")
            all_tputs_keys=$(echo -e  "${all_tputs_keys}\t${tput_keys}")
            all_tputs_cols=$(echo -e  "${all_tputs_cols}\t${tput_cols}")
            all_tputs_bytes=$(echo -e "${all_tputs_bytes}\t${tput_bytes}")
	done

	#sort all tputs
	summed_tputs_ops=$(echo   $all_tputs_ops   | sed 's/ /\n/g' | awk '{ sum = sum + $1 } END { print sum }')
	summed_tputs_keys=$(echo  $all_tputs_keys  | sed 's/ /\n/g' | awk '{ sum = sum + $1 } END { print sum }')
	summed_tputs_cols=$(echo  $all_tputs_cols  | sed 's/ /\n/g' | awk '{ sum = sum + $1 } END { print sum }')
	summed_tputs_bytes=$(echo $all_tputs_bytes | sed 's/ /\n/g' | awk '{ sum = sum + $1 } END { print sum }')

	echo $summed_tputs_ops   > ${system_full}/${control_var}.sum.ops
	echo $summed_tputs_keys  > ${system_full}/${control_var}.sum.keys
	echo $summed_tputs_cols  > ${system_full}/${control_var}.sum.cols
	echo $summed_tputs_bytes > ${system_full}/${control_var}.sum.bytes
    done
    echo "  Combining Clients: complete      "

done
