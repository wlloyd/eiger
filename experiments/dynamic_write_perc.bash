#!/bin/bash
#Dynamic Workload

set -u

dynamic_dir=$(echo $0 | awk -F"dynamic" '{ print "dynamic_"$2 }' | sed 's/.bash//g')

if [ "$dynamic_dir" == "" ]; then
    echo "autodetect of exp name failed, set dynamic dir manyally"
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
    # # Local MBP

    # #!! server should be cmdline option
    # server=localhost

    # #location specific config
    # cops_dir="/Users/wlloyd/Documents/widekv/cassandra2"
    # kill_all_cmd="${cops_dir}/kill_all_cassandra.bash"
    # stress_killer="${cops_dir}/experiments/kill_stress_local_mbp.bash"

    # cluster_start_cmd() {
    # 	cd ${cops_dir};
    # 	./kill_all_cassandra.bash;
    # 	sleep 1;
    # 	./cassandra_dc_launcher.bash 1 1 wait;
    # 	cd -;
    # }
    echo "todo"
    exit
elif [ "$machine_name" == "node2.princeton.vicci.org" ]; then
    # VICCI, run from Princeton 1

    if [ $# -ne 1 ]; then
	echo "$0: [# servers]"
	exit
    fi

    nservers=$1
    dcl_config=${nservers}_in_princeton
    client_config=${nservers}_clients_in_princeton

    #location specific config
    cops_dir="/home/princeton_cops/cops2"
    vanilla_dir="/home/princeton_cops/cassandra-vanilla"
    tools_dir="/home/princeton_cops/cassandra-vanilla"
    exp_dir="${cops_dir}/experiments"
    stress_dir="${cops_dir}/tools/stress"
    output_dir_base="${exp_dir}/${dynamic_dir}"
    exp_uid=$(date +%s)
    output_dir="${output_dir_base}/${exp_uid}"
    mkdir -p ${output_dir}
    rm $output_dir_base/latest
    ln -s $output_dir $output_dir_base/latest 



    dcl_config_full="${cops_dir}/vicci_dcl_config/${dcl_config}"

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

    #kill_all_cmd="${cops_dir}/vicci_cassandra_killer.bash ${dcl_config_full}"
    kill_all_cmd="${cops_dir}/vicci_cassandra_killer.bash ${cops_dir}/vicci_dcl_config/${dcl_config}"
    stress_killer="${cops_dir}/kill_stress_vicci.bash"

    
    source $exp_dir/dynamic_common

else
    echo "Unknown machine_name: ${machine_name}"
    exit
fi



#######################################
#
# Actual Experiment
#
#######################################

#be careful not to set total_keys too low, it creates pathological old version retention if we do that
source ${exp_dir}/dynamic_defaults



# Test: Latency, Throughput of different operations
# Control: # of dependencies

# fixed parameters
run_time=60
trim=15


echo -e "STARTING $0 $@" >> ~/cops2/experiments/progress

num_trials=5
for trial in $(seq $num_trials); do
    #100K columns seems to break stuff
    #for sizeANDkeys in 1:3200000 10:3200000 100:3200000 1000:3200000 10000:320000 100000:32000; do
    #for write_frac in 0.0 0.1 0.2 0.4 0.8 1.0; do

    # value_size=1
    # keys_per_write=1
    # write_trans_frac=0.0
    # total_keys=200000
    # for write_frac in 1.0; do

    #for write_frac in 0.0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0; do
    #for write_frac in 0.0 0.1 0.2 0.4 0.8 1.0; do
    for write_frac in 0.0 0.1 0.2 0.4 0.6 0.8 1.0; do
	variable=$write_frac

	echo -e "Running $0\t$variable at $(date)" >> ~/cops2/experiments/progress



	cops_cluster_start_cmd
	cops_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

        cops_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	$kill_all_cmd


	vanilla_cluster_start_cmd
	vanilla_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

	vanilla_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	$kill_all_cmd

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
