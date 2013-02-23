#!/bin/bash

set -u

if [ $# -ne 3 ]; then
    echo "Usage: "$0" [output_dir] [run_length] [trim_length]"
    exit
fi

output_dir=$1
run_length=$2
trim=$3

used_length=$((run_length - trim - trim))
unset run_length

cd $output_dir


#extract the average throughput from the trimmed part of each run to +tput files
for f in $(ls *+data); do
    base_file_name=$(echo $f | sed 's/+data//')

    #trim to run_length
    line_count=$(cat $f | wc -l)

    #ensure number of lines is enough we can trim and still have run_length data left
    if [[ $line_count -lt $((2 + used_length)) ]]; then
        echo; echo "SKIPPING $f: $line_count !< $((2 + used_length))"; echo;
        continue
    fi

    #skip first 2 lines because they aren't data
    #skip next $trim lines
    #then include the next $used_length lines
    #write that into the file+trim
    # also skip the lines related to errors

    trim_file=$base_file_name"+trim"
    cat $f | grep -v " retried 10 times " | grep -v "^$" | head -n $((2 + trim + used_length)) | tail -n $used_length > $trim_file

    #just care about throughput here, so:
    #  extract throughput
    #  total it
    #  average it
    # but we'll get all 4 kinds
    sum_lines='awk { sum = sum + $1 } END { print sum }'

    total_tput_ops=$(cat $trim_file | awk -F"," '{ print $2 }' | awk '{ sum = sum + $1 } END { print sum }')
    avg_tput_ops=$(echo "$total_tput_ops / $used_length" | bc)

    total_tput_keys=$(cat $trim_file | awk -F"," '{ print $3 }' | awk '{ sum = sum + $1 } END { print sum }')
    avg_tput_keys=$(echo "$total_tput_keys / $used_length" | bc)

    total_tput_cols=$(cat $trim_file | awk -F"," '{ print $4 }' | awk '{ sum = sum + $1 } END { print sum }')
    avg_tput_cols=$(echo "$total_tput_cols / $used_length" | bc)

    total_tput_bytes=$(cat $trim_file | awk -F"," '{ print $5 }' | awk '{ sum = sum + $1 } END { print sum }')
    avg_tput_bytes=$(echo "$total_tput_bytes / $used_length" | bc)

    echo $avg_tput_ops $avg_tput_keys $avg_tput_cols $avg_tput_bytes > $base_file_name"+tput"
done


#generate the +graph file from the +tput files
#format of files is XXX+control_var+data
graph_file="all+graph"
echo -n > ${graph_file}
for data_point in $(ls *+*+tput); do
    control_var=$(echo $data_point | awk -F"+" '{ print $2 }')
    all_tput=$(cat $data_point)
    echo -e "${control_var}\t${all_tput}" >> ${graph_file}
done
cat $graph_file | sort -n > tmp
mv tmp $graph_file