#!/bin/bash
#Dynamic Workload

set -u

dynamic_dir=facebook

#######################################
#
# Cluster Setup
#
#######################################

# MacroBenchmark: Big Clusters

set -x

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


kill_all_cmd="${cops_dir}/vicci_cassandra_killer.bash ${cops_dir}/vicci_dcl_config/${dcl_config}"
stress_killer="${cops_dir}/kill_stress_vicci.bash"


source $exp_dir/dynamic_common


# Modified from from dynamic_common
cops_fb_populate_cluster() {
    fb_populate_cluster $cops_dir $@
}

vanilla_fb_populate_cluster() {
    fb_populate_cluster $vanilla_dir $@
}

    #all_servers is set above
    #num_clients is set above
    #all_clients is set above
fb_populate_cluster() {
    src_dir=$1
    total_keys=$2


    #set the keyspace
    for i in $(seq 3); do
	first_dc_servers_csv=$(echo ${servers_by_dc[0]} | sed 's/ /,/g')

	# set up a killall for stress in case it hangs
	(sleep 60; killall stress) &
	killall_jck_pid=$!
	cd /home/princeton_cops
	${src_dir}/tools/stress/bin/stress --nodes=$first_dc_servers_csv --just-create-keyspace --replication-strategy=NetworkTopologyStrategy --strategy-properties=$strategy_properties
	kill $killall_jck_pid
	sleep 5
    done


    populate_attempts=0
    while [ 1 ]; do

        KILLALL_SSH_TIME=90
	MAX_ATTEMPTS=10
	(sleep $KILLALL_SSH_TIME; killall ssh) &
	killall_ssh_pid=$!

	#let the clients populate the cluster in parallel

	#divide keys across first cluster clients only
	keys_per_client=$((total_keys / num_clients_per_dc))

	pop_pids=""
	for dc in 0; do
	    local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')

	    for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
		client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)

	        #all_servers_csv=$(echo $all_servers | sed 's/ /,/g')
		first_dc_servers_csv=$(echo ${servers_by_dc[0]} | sed 's/ /,/g')

		ssh $client -o StrictHostKeyChecking=no "\
mkdir -p ~/${dynamic_dir}/${exp_uid}; \
$stress_killer; sleep 1; \
cd /home/princeton_cops/; \
cd ${src_dir}/tools/stress; \
bin/stress \
--nodes=$first_dc_servers_csv \
--operation=FACEBOOK_POPULATE \
--consistency-level=LOCAL_QUORUM \
--replication-strategy=NetworkTopologyStrategy \
--strategy-properties=$strategy_properties \
--num-different-keys=$keys_per_client \
--num-keys=$keys_per_client \
--stress-index=$cli_index \
--stress-count=$num_clients_per_dc \
 > >(tee ~/${dynamic_dir}/${exp_uid}/populate.out) \
2> >(tee ~/${dynamic_dir}/${exp_uid}/populate.err) \
" 2>&1 | awk '{ print "'$client': "$0 }' &
		pop_pid=$!
		pop_pids="$pop_pids $pop_pid"
	    done
	done

	#wait for clients to finish
	for pop_pid in $pop_pids; do
	    echo "Waiting on $pop_pid"
	    wait $pop_pid
	done

	sleep 1

	kill $killall_ssh_pid
	# if we kill killall successfully, it will return 0 and that means we populated the cluster and can continue
	#  otherwise we try again
	killed_killall=$?

	if [ $killed_killall == "0" ]; then
	   break;
	fi
	((populate_attempts++))
	if [[ $populate_attempts -ge $MAX_ATTEMPTS ]]; then
	   echo -e "\n\n \e[01;31m Could not populate the cluster after $MAX_ATTEMPTS attempts \e[0m \n\n"
	   exit;
	fi
	echo -e "\e[01;31m Failed populating $populate_attempts times, trying again (out of $MAX_ATTEMPTS) \e[0m"
      done
    }


    cops_fb_run_experiment() {
	fb_run_experiment $@ $cops_dir cops2
    }

    vanilla_fb_run_experiment() {
	fb_run_experiment $@ $vanilla_dir vanilla
    }

    #all_servers is set above
    #num_clients is set above
    #all_clients is set above
    fb_run_experiment() {
	total_keys=$1
	write_trans_frac=$2
	exp_time=$3
	variable=$4
	trial=$5
	src_dir=$6
	cluster_type=$7

	cli_output_dir="~/${dynamic_dir}/${exp_uid}/${cluster_type}/trial${trial}"
	data_file_name=$1_$2_$3+${4}+data

	#for dc in $(seq 0 $((num_dcs - 1))); do
	for dc in 0; do
	    local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')

	    for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
		client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)

		ssh $client -o StrictHostKeyChecking=no "\
mkdir -p $cli_output_dir; \
cd /home/princeton_cops/;
cd ${src_dir}/tools/stress; \
((bin/stress \
--progress-interval=1 \
--nodes=$local_servers_csv \
--operation=FACEBOOK \
--consistency-level=LOCAL_QUORUM \
--replication-strategy=NetworkTopologyStrategy \
--strategy-properties=$strategy_properties \
--num-different-keys=$total_keys \
--stress-index=$cli_index \
--stress-count=$num_clients_per_dc \
--num-keys=2000000 \
--write-transaction-fraction=$write_trans_frac \
--threads=64 \
 > >(tee ${cli_output_dir}/${data_file_name}) \
2> ${cli_output_dir}/${data_file_name}.stderr \
) &); \
sleep $((exp_time + 10)); ${src_dir}/kill_stress_vicci.bash" \
2>&1 | awk '{ print "'$client': "$0 }' &
	    done
	done

	#wait for clients to finish
	wait
    }



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
    for write_trans_frac in 0.0 1.0; do
	variable=$write_trans_frac

	echo -e "Running $0\t$variable at $(date)" >> ~/cops2/experiments/progress

	cops_cluster_start_cmd
	cops_fb_populate_cluster ${total_keys}

        cops_fb_run_experiment $total_keys $write_trans_frac $run_time $variable $trial

	$kill_all_cmd


	if [ $write_trans_frac == "0.0" ]; then
	    vanilla_cluster_start_cmd
	    vanilla_fb_populate_cluster ${total_keys}

	    vanilla_fb_run_experiment $total_keys $write_trans_frac $run_time $variable $trial

	    $kill_all_cmd
	fi

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



