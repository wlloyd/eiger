#!/bin/bash
# Dep propagation
# MicroBenchmark latency and through as we increase # of dependencies

set -u


#######################################
#
# Cluster Setup
#
#######################################

# MicroBenchmark => 1 dc, 1 machine only

set -x

# Setup differs depending on where I launch this from
machine_name=$(uname -n)
if [ "$machine_name" == "wlloyds-macbook-pro.local" ]; then
    # Local MBP

    #!! server should be cmdline option
    server=localhost

    #location specific config
    cops_dir="/Users/wlloyd/Documents/widekv/cassandra2"
    kill_all_cmd="${cops_dir}/kill_all_cassandra.bash"
    stress_killer="${cops_dir}/experiments/kill_stress_local_mbp.bash"

    cluster_start_cmd() {
	cd ${cops_dir};
	./kill_all_cassandra.bash;
	sleep 1;
	./cassandra_dc_launcher.bash 1 1 wait;
	cd -;
    }
elif [ "$machine_name" == "node1.princeton.vicci.org" ]; then
    # VICCI, run from Princeton 1

    dcl_config=2_in_princeton
    server=node5.princeton.vicci.org

    #location specific config
    cops_dir="/home/princeton_cops/cops2"
    dcl_config_full="${cops_dir}/vicci_dcl_config/${dcl_config}"
    kill_all_cmd="${cops_dir}/vicci_cassandra_killer.bash ${dcl_config_full}"
    stress_killer="${cops_dir}/experiments/kill_stress_vicci.bash"

    #get cluster up an running
    cluster_start_cmd() {
	cd ${cops_dir};
	$kill_all_cmd;
	sleep 1;
	while [ 1 ]; do
	    ./vicci_dc_launcher.bash ${dcl_config_full}
	    return_value=$?
	    if [ $return_value -eq 0 ]; then
		break
	    fi
	done
	cd -;
    }
else
    echo "Unknown machine_name: ${machine_name}"
    exit
fi


exp_dir="${cops_dir}/experiments"
stress_dir="${cops_dir}/tools/stress"
output_dir_base="${exp_dir}/dep_propagation"
output_dir="${output_dir_base}/$(date +%s)"
mkdir -p ${output_dir}
rm $output_dir_base/latest
ln -s $output_dir $output_dir_base/latest 



#######################################
#
# Actual Experiment
#
#######################################

# Test: Latency, Throughput of different operations
# Control: # of dependencies

# fixed parameters
run_length=12
trim=1

# Operation parameters [operation]:[# cols]:[col size (bytes)]
#lines="INSERT:1:1 INSERT:10:1 INSERT:100:1 INSERT:1:1000 INSERT:10:1000 INSERT:100:1000 INSERT:1:10000 INSERT:10:10000 INSERT:100:10000"
#lines="INSERT:1:1 INSERT:10:1 INSERT:1:100 INSERT:10:100 INSERT:1:1000 INSERT:10:1000 INSERT:1:10000 INSERT:10:10000"
lines="INSERT:1:1 INSERT:1:1024 INSERT:8:256 INSERT:8:512 INSERT:8:1024"
#lines="INSERT:1:1 INSERT:10:1 INSERT:1:100 INSERT:10:100"
#lines="INSERT:1:1 INSERT:10:1"

num_trials=5

for trial in $(seq $num_trials); do
    output_dir_exp="${output_dir}/trial${trial}"
    mkdir $output_dir_exp
    for line in $lines; do
        for num_deps in 0 1 2 4 8 16 32 64 128 256 512 1024; do
        #for num_deps in 0 1 10 100; do
	#for num_deps in 0 1; do


	    cluster_start_cmd

	    
            op=$(echo $line | awk -F":" '{ print $1 }')
            num_cols=$(echo $line | awk -F":" '{ print $2 }')
            val_size=$(echo $line | awk -F":" '{ print $3 }')

        #output file format: fixed parameters separated by underscores, then a dot, the control variable, then .data
            output_file="${op}_${num_cols}_${val_size}.${num_deps}.data"
            echo -e "op=${op}\tnum_cols=${num_cols}\tval_size=${val_size}\tnum_deps=${num_deps} > ${output_file}"


        #unused, but potentially interesting, stress options:
#     --num-different-keys=NUM-DIFFERENT-KEY
#     --cardinality=CARDINALITY
#     --strategy-properties=STRATEGY-PROPERTIES
#     --average-size-values
#     --consistency-level=CONSISTENCY-LEVEL
#     --replication-factor=REPLICATION-FACTOR
#     --supercolumns=SUPERCOLUMNS
#     --threads=THREADS
#     --stdev=STDEV
#     --random
#     --family-type=FAMILY-TYPE
#     --create-index=CREATE-INDEX

            set -x
            cd ${stress_dir}
            (bin/stress \
		--progress-interval=1 \
		--columns=${num_cols} \
		--column-size=${val_size} \
		--nodes=${server} \
		--operation=${op} \
		--num-keys=2000000000 \
		--num-dependencies=${num_deps} \
		> ${output_dir_exp}/${output_file} \
		) &
            stress_pid=$!
            
            sleep $((run_length + 10))

        #kill the entire process group
	    ${stress_killer} $stress_pid

            cd ${exp_dir}
            set +x


	    $kill_all_cmd

	done
    done
done



#######################################
#
# Cleanup Experiment
#
#######################################
set +x
eval $kill_all_cmd
set -x

#######################################
#
# Process Output
#
#######################################
${exp_dir}/dep_propagation_postprocess_full.bash ${exp_dir} $num_trials ${output_dir} ${run_length} ${trim}


