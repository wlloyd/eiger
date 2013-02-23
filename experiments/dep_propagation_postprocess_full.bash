#!/bin/bash

set -u

if [ $# -ne 5 ]; then
    echo "Usage: "$0" [exp_dir] [num_trials] [output_dir] [run_length] [trim_length]"
    exit
fi

exp_dir=$1
num_trials=$2
output_dir=$3
run_length=$4
trim=$5

for trial in $(seq $num_trials); do
    output_dir_exp="${output_dir}/trial${trial}"
    ${exp_dir}/dep_propagation_postprocess_trial.bash ${output_dir_exp} ${run_length} ${trim}

    echo; echo; echo "TRIAL $trial"
    tail $output_dir_exp/*.graph
done

${exp_dir}/dep_propagation_postprocess_combine.bash ${output_dir}

echo; echo; echo "COMBINED"
tail $output_dir/*.graph



