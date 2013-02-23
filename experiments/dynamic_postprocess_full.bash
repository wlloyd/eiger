#!/bin/bash

set -u

if [ $# -lt 4 ]; then
    echo "Usage: "$0" [exp_dir] [output_dir] [run time] [trim] {shuffle}"
    exit
fi

exp_dir=$1
output_dir=$2
run_length=$3
trim=$4

if  [ $# -gt 4 ]; then
    ${exp_dir}/dynamic_shuffle_dirs.bash ${output_dir}
fi

${exp_dir}/dynamic_postprocess_combine_trials.bash ${exp_dir} ${output_dir} ${run_length} ${trim}
${exp_dir}/dynamic_per_client_tput.bash ${output_dir}

cd ${output_dir}
#tar -cjf trials.tar.bz2 trial*
#mv trial? .trash