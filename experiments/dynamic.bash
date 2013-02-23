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

    if [ $# -ne 2 ]; then
	echo "$0: [dcl_config] [client_config]"
	exit
    fi

    #location specific config
    cops_dir="/home/princeton_cops/cops2"
    exp_dir="${cops_dir}/experiments"
    stress_dir="${cops_dir}/tools/stress"
    output_dir_base="${exp_dir}/${dynamic_dir}"
    exp_uid=$(date +%s)
    output_dir="${output_dir_base}/${exp_uid}"
    mkdir -p ${output_dir}
    rm $output_dir_base/latest
    ln -s $output_dir $output_dir_base/latest 


    dcl_config=$1
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


    client_config=$2
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

    kill_all_cmd="${cops_dir}/vicci_cassandra_killer.bash ${dcl_config_full}"
    stress_killer="${cops_dir}/kill_stress_vicci.bash"


    source dynamic_common


<<<<<<< HEAD

else
    echo "Unknown machine_name: ${machine_name}"
    exit
fi
=======
	    #write to ALL so the cluster is populated everywhere
	    ssh $client -o StrictHostKeyChecking=no "\
mkdir -p ~/${dynamic_dir}/${exp_uid}; \
$stress_killer; sleep 1; \
cd ${src_dir}/tools/stress; \
bin/stress \
--nodes=$first_dc_servers_csv \
--columns=$max_columns \
--column-size=$column_size \
--operation=${insert_cmd} \
--consistency-level=LOCAL_QUORUM \
--replication-strategy=NetworkTopologyStrategy \
--strategy-properties=$strategy_properties \
--num-different-keys=$keys_per_client \
--num-keys=$keys_per_client \
--stress-index=$index \
--stress-count=$num_clients \
> ~/${dynamic_dir}/${exp_uid}/populate.out" &


	done

	#wait for clients to finish
	for index in $(seq 0 $((num_clients - 1))); do
	    fg
	done

	set +m
    }


    cops_run_experiment() {
	internal_run_experiment $@ $cops_dir cops2
    }

    vanilla_run_experiment() {
	internal_run_experiment $@ $vanilla_dir vanilla
    }

    #all_servers is set above
    #num_clients is set above
    #all_clients is set above
    internal_run_experiment() {
	total_keys=$1
	column_size=$2
	cols_per_key_read=$3
	cols_per_key_write=$4
	keys_per_read=$5
	keys_per_write=$6
	write_frac=$7
	write_trans_frac=$8
	exp_time=$9
	variable=${10}
	trial=${11}
	src_dir=${12}
	cluster_type=${13}

	cli_output_dir="~/${dynamic_dir}/${exp_uid}/${cluster_type}/trial${trial}"
	data_file_name=$1_$2_$3_$4_$5_$6_$7_$8_$9+${10}+data

	#divide keys across all clients
	keys_per_client=$((total_keys / num_clients))

	set -m # need monitor mode to fg tasks

	#for dc in $(seq 0 $((num_dcs - 1))); do
	for dc in 0; do
	    local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')

	    for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
		client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)
		
		ssh $client -o StrictHostKeyChecking=no "\
mkdir -p $cli_output_dir; \
cd ${src_dir}/tools/stress; \
((bin/stress \
--progress-interval=1 \
--nodes=$local_servers_csv \
--operation=DYNAMIC \
--consistency-level=LOCAL_QUORUM \
--replication-strategy=NetworkTopologyStrategy \
--strategy-properties=$strategy_properties \
--num-different-keys=$keys_per_client \
--stress-index=$cli_index \
--stress-count=$num_clients_per_dc \
--num-keys=2000000 \
--column-size=$column_size \
--columns-per-key-read=$cols_per_key_read \
--columns-per-key-write=$cols_per_key_write \
--keys-per-read=$keys_per_read \
--keys-per-write=$keys_per_write \
--write-fraction=$write_frac \
--write-transaction-fraction=$write_trans_frac \
--threads=16 \
| tee ${cli_output_dir}/${data_file_name}) &); sleep $((exp_time + 10)); ${exp_dir}/kill_stress_vicci.bash" &
	    done
	done
>>>>>>> 5cc13e9709b03d3ef9f09416a1bc5ec9e24f95d1


source dynamic_defaults



<<<<<<< HEAD
=======
    gather_results() {
	for dc in 0; do
            for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
		client_dir=${output_dir}/client${cli_index}
		client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)
		rsync -az $client:${dynamic_dir}/${exp_uid}/* ${client_dir}
	    done
	done
    }
>>>>>>> 5cc13e9709b03d3ef9f09416a1bc5ec9e24f95d1



#######################################
#
# Actual Experiment
#
#######################################

# Test: Latency, Throughput of different operations
# Control: # of dependencies

<<<<<<< HEAD
# fixed parameters
run_length=40
trim=5

num_trials=2

for trial in $(seq 1 $num_trials); do 
    for write_frac in 0.0 0.1 0.2 0.4 0.8 1.0; do
	variable=$write_frac
=======

#######################################
#
# Actual Experiment
#
#######################################

nservers=2
#be careful not to set total_keys too low, it creates pathological old version retention if we do that
total_keys=$((nservers*1000*1000))

value_size=1
cols_per_key_read=10
cols_per_key_write=10
keys_per_read=10
keys_per_write=10
write_frac=0.1
write_trans_frac=0.0
run_time=60
trial=1
>>>>>>> 5cc13e9709b03d3ef9f09416a1bc5ec9e24f95d1

    done
done

# Test: Latency, Throughput of different operations
# Control: # of dependencies

<<<<<<< HEAD
cops_cluster_start_cmd
cops_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size}

#run_experiment keys cols_size cpkr cpkw kpr kpw wf wtf time var trial#
#run_experiment $((100*1000)) 1 1 1 1 1 .1 0.0 15 .1 1
cops_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

$kill_all_cmd


vanilla_cluster_start_cmd
vanilla_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size}

#run_experiment keys cols_size cpkr cpkw kpr kpw wf wtf time var trial#
#run_experiment $((100*1000)) 1 1 1 1 1 .1 0.0 15 .1 1
vanilla_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

$kill_all_cmd

gather_results

=======
# fixed parameters
run_time=40
trim=5

num_trials=1
for trial in $(seq $num_trials); do
    for write_frac in 0.0 0.1 0.2 0.4 0.8 1.0; do
	variable=$write_frac
>>>>>>> 5cc13e9709b03d3ef9f09416a1bc5ec9e24f95d1

	cops_cluster_start_cmd
	cops_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

<<<<<<< HEAD

=======
        cops_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	$kill_all_cmd
>>>>>>> 5cc13e9709b03d3ef9f09416a1bc5ec9e24f95d1


	vanilla_cluster_start_cmd
	vanilla_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

	vanilla_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	$kill_all_cmd

	gather_results
    done
done

exit



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
#${exp_dir}/dep_propagation_postprocess_full.bash ${exp_dir} $num_trials ${output_dir} ${run_time} ${trim}


