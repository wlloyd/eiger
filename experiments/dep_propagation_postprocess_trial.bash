#!/bin/bash

set -u

if [ $# -ne 3 ]; then
    echo "Usage: "$0" [output_dir] [run_length] [trim_length]"
    exit
fi

output_dir=$1
run_length=$2
trim=$3


cd $output_dir


#extract the average throughput from the trimmed part of each run to .tput files
for f in $(ls *.data); do
    base_file_name=$(echo $f | sed 's/.data//')

    #trim to run_length
    line_count=$(cat $f | wc -l)

    #ensure number of lines is enough we can trim and still have run_length data left
    if [[ $line_count -lt $((2 + run_length)) ]]; then
        echo; echo "SKIPPING $f: $line_count !< $((2 + run_length))"; echo;
        continue
    fi

    #skip first 2 lines because they aren't data
    #skip next $trim lines
    #then include the next $run_length lines
    #write that into the file.trim

    trim_file=$base_file_name".trim"
    cat $f | head -n $((2 + trim + run_length)) | tail -n $run_length > $trim_file

    #just care about throughput here, so:
    #  extract throughput
    #  total it
    #  average it
    sum_lines='awk { sum = sum + $1 } END { print sum }'
    total_tput=$(cat $trim_file | awk -F"," '{ print $2 }' | awk '{ sum = sum + $1 } END { print sum }')
    avg_tput=$(echo "$total_tput / $run_length" | bc)
    echo $avg_tput  > $base_file_name".tput"
done


#generate the .graph file from the .tput files
#format of files is XXX.control_var.data
graph_lines=$(ls *.data | awk -F"." '{ print $1 }' | sort -u)
for graph_line in $graph_lines; do
    graph_file=${graph_line}".graph"
    echo -n > ${graph_file}
    for data_point in $(ls $graph_line.*.tput); do
        control_var=$(echo $data_point | awk -F"." '{ print $2 }')
        tput=$(cat $data_point)
        echo -e "${control_var}\t${tput}" >> ${graph_file}
    done
    cat $graph_file | sort -n > tmp
    mv tmp $graph_file
done