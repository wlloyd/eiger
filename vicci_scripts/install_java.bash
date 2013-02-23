#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 [hosts file]"
    exit
fi

host_file=$1
num_hosts=$(cat $host_file | wc -l)
timeout=60

remote_path=/home/princeton_cops/

#pssh -h $host_file -p $num_hosts -t $timeout -o pssh_stdout \
#    -e pssh_stderr "sudo yum install ant-* -y"

for host in $(cat $host_file); do 
    echo $host
    ssh -t -t -o StrictHostKeyChecking=no $host "sudo yum install java-1.6.0-openjdk -y" &
    #ssh -t -t -o StrictHostKeyChecking=no $host "mkdir ~/cassandra_var; mkdir ~/cassandra_var/data; mkdir ~/cassandra_var/commit-log; mkdir ~/cassandra_var/saved_caches"
done