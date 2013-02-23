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

#process all trials
for trial_full in $(ls -dp ${output_dir}/* | grep '/$'); do
    echo $trial_full
    ${exp_dir}/dynamic_postprocess_combine_clients.bash $exp_dir $trial_full $run_length $trim
done


for system in cops2 vanilla; do
    all_file=$output_dir/$system".all"
    graph_file=$output_dir/$system".graph"

    echo -n > $all_file.ops
    echo -n > $all_file.keys
    echo -n > $all_file.cols
    echo -n > $all_file.bytes
    echo -n > $graph_file.ops
    echo -n > $graph_file.keys
    echo -n > $graph_file.cols
    echo -n > $graph_file.bytes
    echo -n > $graph_file

    for data_point_full in $(ls ${output_dir}/trial1/$system/*.sum.ops); do
	data_point=$(echo $data_point_full | awk -F"/" '{ print $NF }'| sed 's/.ops//')
	control_var=$(echo $data_point | awk -F".sum" '{ print $1 }')

	all_tputs_ops=""
	all_tputs_keys=""
	all_tputs_cols=""
	all_tputs_bytes=""
	for trial_full in $(ls -dp ${output_dir}/* | grep '/$'); do
	    tput_ops=$(cat   ${trial_full}/${system}/$data_point.ops   | awk '{ print $1 }')
	    tput_keys=$(cat  ${trial_full}/${system}/$data_point.keys  | awk '{ print $1 }')
	    tput_cols=$(cat  ${trial_full}/${system}/$data_point.cols  | awk '{ print $1 }')
	    tput_bytes=$(cat ${trial_full}/${system}/$data_point.bytes | awk '{ print $1 }')
            all_tputs_ops=$(echo -e   "${all_tputs_ops}\t${tput_ops}")
            all_tputs_keys=$(echo -e  "${all_tputs_keys}\t${tput_keys}")
            all_tputs_cols=$(echo -e  "${all_tputs_cols}\t${tput_cols}")
            all_tputs_bytes=$(echo -e "${all_tputs_bytes}\t${tput_bytes}")
	done

	#sort all tputs
	all_tputs_ops=$(echo   $all_tputs_ops   | sed 's/ /\n/g' | sort -n | xargs)
	all_tputs_keys=$(echo  $all_tputs_keys  | sed 's/ /\n/g' | sort -n | xargs)
	all_tputs_cols=$(echo  $all_tputs_cols  | sed 's/ /\n/g' | sort -n | xargs)
	all_tputs_bytes=$(echo $all_tputs_bytes | sed 's/ /\n/g' | sort -n | xargs)

	num_tputs_ops=$(echo   $all_tputs_ops   | wc -w)
	num_tputs_keys=$(echo  $all_tputs_keys  | wc -w)
	num_tputs_cols=$(echo  $all_tputs_cols  | wc -w)
	num_tputs_bytes=$(echo $all_tputs_bytes | wc -w)

	median_tput_line_ops=$(echo   "(${num_tputs_ops}+1)/2" | bc)
	median_tput_line_keys=$(echo  "(${num_tputs_keys}+1)/2" | bc)
	median_tput_line_cols=$(echo  "(${num_tputs_cols}+1)/2" | bc)
	median_tput_line_bytes=$(echo "(${num_tputs_bytes}+1)/2" | bc)

	median_tput_ops=$(echo   $all_tputs_ops   | awk '{ print $'$median_tput_line_ops' }')
	median_tput_keys=$(echo  $all_tputs_keys  | awk '{ print $'$median_tput_line_keys' }')
	median_tput_cols=$(echo  $all_tputs_cols  | awk '{ print $'$median_tput_line_cols' }')
	median_tput_bytes=$(echo $all_tputs_bytes | awk '{ print $'$median_tput_line_bytes' }')


	echo -e "${control_var}\t${all_tputs_ops}"   >> ${all_file}.ops
	echo -e "${control_var}\t${all_tputs_keys}"  >> ${all_file}.keys
	echo -e "${control_var}\t${all_tputs_cols}"  >> ${all_file}.cols
	echo -e "${control_var}\t${all_tputs_bytes}" >> ${all_file}.bytes

	echo -e "${control_var}\t${median_tput_ops}"   >> ${graph_file}.ops
	echo -e "${control_var}\t${median_tput_keys}"  >> ${graph_file}.keys
	echo -e "${control_var}\t${median_tput_cols}"  >> ${graph_file}.cols
	echo -e "${control_var}\t${median_tput_bytes}" >> ${graph_file}.bytes

	echo -e "${control_var}\t${median_tput_ops}\t${median_tput_keys}\t${median_tput_cols}\t${median_tput_bytes}" >> ${graph_file}
    done

    cat $all_file.ops | sort -n > tmp
    mv tmp $all_file.ops
    cat $all_file.keys | sort -n > tmp
    mv tmp $all_file.keys
    cat $all_file.cols | sort -n > tmp
    mv tmp $all_file.cols
    cat $all_file.bytes | sort -n > tmp
    mv tmp $all_file.bytes

    cat $graph_file.ops | sort -n > tmp
    mv tmp $graph_file.ops
    cat $graph_file.keys | sort -n > tmp
    mv tmp $graph_file.keys
    cat $graph_file.cols | sort -n > tmp
    mv tmp $graph_file.cols
    cat $graph_file.bytes | sort -n > tmp
    mv tmp $graph_file.bytes

    cat $graph_file | sort -n > tmp
    mv tmp $graph_file
    echo $graph_file
    tail $graph_file
done

cd $output_dir
paste cops2.graph vanilla.graph | awk '{ printf("%s\t%.3f\n", $0, $4/$9) }' > combined.graph