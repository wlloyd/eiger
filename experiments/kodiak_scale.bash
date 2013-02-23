#!/bin/bash
#Scaling experiment on kodiak testbed

set -u

dynamic_dir=$(echo $0 | awk -F"/" '{ print $NF }' | sed 's/.bash//g')

if [ "$dynamic_dir" == "" ]; then
    echo "autodetect of exp name failed, set dir manually"
    exit
fi

#######################################
#
# Cluster Setup
#
#######################################

# MacroBenchmark: Big Clusters

set -x

# Setup differs depending on where I launch this from
machine_name=$(uname -n)
if [ "$machine_name" == "wlloyds-macbook-pro.local" ]; then
    echo "todo"
    exit
elif [ "$machine_name" == "node2.princeton.vicci.org" ]; then
    echo "todo vicci"
    exit
elif [ "$(uname -n | grep kodiak)" != "" ]; then
    #location specific config
    cops_dir="/users/wlloyd/cops2"
    vanilla_dir="/users/wlloyd/cassandra-vanilla"
    tools_dir="/users/wlloyd/cassandra-vanilla"
    exp_dir="${cops_dir}/experiments"
    stress_dir="${cops_dir}/tools/stress"
    output_dir_base="${exp_dir}/${dynamic_dir}"
    exp_uid=$(date +%s)
    output_dir="${output_dir_base}/${exp_uid}"
    mkdir -p ${output_dir}
    rm $output_dir_base/latest
    ln -s $output_dir $output_dir_base/latest 

    #pull in the functions that do everything from dynamic_common
    source ${exp_dir}/kodiak_common
else
    echo "Unknown machine_name: ${machine_name}"
    exit
fi



#######################################
#
# Actual Experiment
#
#######################################

source ${exp_dir}/dynamic_defaults

# Test: Latency, Throughput of different operations
# Control: # of dependencies

# fixed parameters
#run_time=150
#trim=60
run_time=30
trim=10

total_keys=10000
value_size=1
cols_per_key_read=1
cols_per_key_write=1
keys_per_read=1
keys_per_write=1
#write_frac=0.1
write_frac=0
write_trans_frac=0

echo -e "STARTING $0 $@" >> ~/cops2/experiments/progress

num_trials=1
for trial in $(seq $num_trials); do
    #for nservers in 64 32 16 8 4 2; do
    #for nservers in 2A 4A 8A 16A; do
    #for nservers in 2A 4A 8A; do
    #for nservers in 2A 8A 32A 64A 128A; do
    #for nservers in 32A 64A 2A 4A 8A 16A; do
    for nservers in 128A; do
	variable=$nservers
	echo -e "Running $0\t$variable at $(date)" >> ~/cops2/experiments/progress

	#total_keys=$((10000*nservers))
	total_keys=$((10000*$(echo $nservers | sed 's/A//g') ))

	dcl_config=kodiak_${nservers}
	client_config=kodiak_${nservers}_clients

	dcl_config_full="${cops_dir}/vicci_dcl_config/${dcl_config}"
	kill_all_cmd="${cops_dir}/kodiak_cassandra_killer.bash ${dcl_config_full}"
	stress_killer="${cops_dir}/kill_stress_vicci.bash"


	all_servers=($(cat $dcl_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs))
	all_servers=$(echo "echo ${all_servers[@]}" | bash)
	num_dcs=$(cat $dcl_config_full | grep num_dcs | awk -F"=" '{ print $2 }')

	strategy_properties="DC0:1"
	for i in $(seq 1 $((num_dcs-1))); do
	    strategy_properties=$(echo ${strategy_properties}",DC${i}:1")
	done

	num_servers=$(echo $all_servers | wc -w)
	num_servers_per_dc=$((num_servers / num_dcs))
	
	for dc in $(seq 0 $((num_dcs-1))); do
	    this_dc_servers=$(echo $all_servers | sed 's/ /\n/g' | head -n $((num_servers_per_dc * (dc+1))) | tail -n $num_servers_per_dc | xargs)
	    servers_by_dc[$dc]=${this_dc_servers}
	done
	echo ${servers_by_dc[@]}



	client_config_full="${cops_dir}/vicci_dcl_config/${client_config}"

	all_clients=$(cat $client_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs)
	all_clients=$(echo "echo ${all_clients[@]}" | bash)

	num_clients=$(echo $all_clients | wc -w)
	num_clients_per_dc=$((num_clients / num_dcs))
	
	for dc in $(seq 0 $((num_dcs-1))); do
	    this_dc_clients=$(echo $all_clients | sed 's/ /\n/g' | head -n $((num_clients_per_dc * (dc+1))) | tail -n $num_clients_per_dc | xargs)
	    clients_by_dc[$dc]=${this_dc_clients}
	done
	echo ${clients_by_dc[@]}





	cops_cluster_start_cmd
	cops_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}
	sleep 10
	cops_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	$kill_all_cmd


	# vanilla_cluster_start_cmd
	# vanilla_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

	# vanilla_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	# $kill_all_cmd

	gather_results
    done
done

echo -e "FINISHED $0\tat $(date)" >> ~/cops2/experiments/progress

#######################################
#
# Cleanup Experiment
#
#######################################
set +x
$kill_all_cmd
set -x

#######################################
#
# Process Output
#
#######################################
cd $exp_dir
./dynamic_postprocess_full.bash . ${output_dir} ${run_time} ${trim} shuffle


