#!/bin/bash

set -u

exp_subdir=$(echo "/"$0 | awk -F"/" '{ print $NF }' | sed 's/.bash//g')

if [ "$exp_subdir" == "" ]; then
    echo "autodetect of exp name failed, set manually"
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
elif [ -n $(echo "$machine_name" | grep vicci) ]; then
    # VICCI, run in the Princeton cluster

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
    output_dir_base="${exp_dir}/${exp_subdir}"
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


#based off dynamic_common's internal_run_experiment
write_txn_experiment() {
    internal_txn_experiment $@ cops2 WRITE_TXN
}

batch_mutate_experiment() {
    internal_txn_experiment $@ vanilla BATCH_MUTATE
}

internal_txn_experiment() {
    total_keys=$1
    column_size=$2
    cols_per_key_write=$3
    keys_per_server=$4
    servers_per_txn=$5
    exp_time=$6
    variable=$7
    trial=$8
    src_dir=$9
    cluster_type=${10}
    operation_type=${11}

    cli_output_dir="~/${exp_subdir}/${exp_uid}/${cluster_type}/trial${trial}"
    data_file_name=$1_$2_$3_$4_$5_$6+$7+data

    #divide keys across all clients
    keys_per_client=$((total_keys / num_clients))

    #for dc in $(seq 0 $((num_dcs - 1))); do
    for dc in 0; do
	local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')

	for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
	    client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)

	    ssh $client -o StrictHostKeyChecking=no "\
mkdir -p $cli_output_dir; \
cd /home/princeton_cops/; \
cd ${src_dir}/tools/stress; \
((bin/stress \
--progress-interval=1 \
--nodes=$local_servers_csv \
--operation=$operation_type \
--consistency-level=LOCAL_QUORUM \
--replication-strategy=NetworkTopologyStrategy \
--strategy-properties=$strategy_properties \
--num-different-keys=$keys_per_client \
--stress-index=$cli_index \
--stress-count=$num_clients_per_dc \
--num-keys=2000000 \
--column-size=$column_size \
--columns-per-key-write=$cols_per_key_write \
--num-servers=$num_servers_per_dc \
--keys-per-server=$keys_per_server \
--servers-per-txn=$servers_per_txn \
--threads=32 \
2>${cli_output_dir}/${data_file_name}.stderr | tee ${cli_output_dir}/${data_file_name}) &); \
sleep $((exp_time + 30)); ${src_dir}/kill_stress_vicci.bash" &
#note: we need to give write_txns extra time to finish because they have to calculate a lot of md5 before they actually begin
	done
    done



# WRITE_TXN \

    #wait for clients to finish
    wait
}



#######################################
#
# Actual Experiment
#
#######################################

source ${exp_dir}/dynamic_defaults

#be careful not to set total_keys too low, it creates pathological old version retention if we do that
#total_keys=3200000
total_keys=200000

##Temporarily setthing this lower to make sure I'm not overwhelming the effect of the write txn with large value size
#value_size=1

# Test: Latency, Throughput of different operations
# Control: # of dependencies

# fixed parameters
run_time=60
# NOTE: if experiments aren't running to completion, it's probably
# because the extra sleep time before killing it's long enough for all
# the md5 to be calculated
trim=15

allKPSandSPT=""

for kps in 1 5 10; do
#for kps in 10; do
    for spt in $(seq 1 $num_servers_per_dc); do
    #for spt in 4 5 6 7 8; do
	allKPSandSPT="$allKPSandSPT $kps:$spt"
    done
done

echo -e "STARTING $0 $@" >> ~/cops2/experiments/progress

num_trials=5
for trial in $(seq $num_trials); do
    # note i could make this faster by splitting them up, and doing the vanilla comparison only once per kps
    for kpsANDspt in $allKPSandSPT; do

	keys_per_server=$(echo $kpsANDspt | awk -F":" '{ print $1 }')
	servers_per_txn=$(echo $kpsANDspt | awk -F":" '{ print $2 }')
	variable=$kpsANDspt

	echo -e "Running $0\t$variable at $(date)" >> ~/cops2/experiments/progress

	# required for any use of functions in dynamic_common
	dynamic_dir=$exp_subdir

	cops_cluster_start_cmd
	cops_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}
        #cops_write_txn_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial
	write_txn_experiment $total_keys $value_size $cols_per_key_write $keys_per_server $servers_per_txn $run_time $variable $trial $cops_dir

	$kill_all_cmd



	vanilla_cluster_start_cmd
	vanilla_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

	batch_mutate_experiment $total_keys $value_size $cols_per_key_write $keys_per_server $servers_per_txn $run_time $variable $trial $vanilla_dir

	# write_frac=1.0
	# write_trans_frac=0.0
	# keys_per_write=$keys_per_server
	# cols_per_key_read=10000
	# keys_per_read=10000
	# vanilla_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	$kill_all_cmd

	gather_results
    done
    echo "---- Trial $trial completed ----"
done
echo "---- All trials completed ----"


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
