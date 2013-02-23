#!/bin/bash

set -u

#note: Set ~/.ssh/config to 
#StrictHostKeyChecking=no
#UserKnownHostsFile=/dev/null
#GlobalKnownHostsFile=/dev/null

if [ $# -ne 1 ]; then
    echo "Usage: $0 [hosts file]"
    exit
fi

host_file=$1
num_hosts=$(cat $host_file | wc -l)
#timeout=60
timeout=300

remote_path=/home/princeton_cops/

#note: the cops_bin file contains links to the actual binaries, so we
#don't need to copy them into that folder each time

#copy will fail if the binaries are currently running
#pssh -h $host_file -p $num_hosts -t $timeout -o pssh_stdout \
#    -e pssh_stderr "killall -9 backend; killall -9 manager; killall -9 benchmarker; killall -9 macro_benchmarker;"

sleep 3

set -x
echo > cops_bin_push.tmp
prsync -O "StrictHostKeyChecking=no" -x "--exclude 'CommitLog' --exclude 'experiments'" -h $host_file -p $num_hosts -t $timeout -o pssh_stdout \
    -e pssh_stderr -r /home/princeton_cops/cops2 /home/princeton_cops/ > >(tee -a cops_bin_push.tmp) 2>&1
prsync -O "StrictHostKeyChecking=no" -x "--exclude 'CommitLog'" -h $host_file -p $num_hosts -t $timeout -o pssh_stdout \
    -e pssh_stderr -r /home/princeton_cops/cassandra-vanilla /home/princeton_cops/ > >(tee -a cops_bin_push.tmp) 2>&1
prsync -O "StrictHostKeyChecking=no" -x "--exclude 'CommitLog'" -h $host_file -p $num_hosts -t $timeout -o pssh_stdout \
    -e pssh_stderr -r /home/princeton_cops/vicci_tools /home/princeton_cops/ > >(tee -a cops_bin_push.tmp) 2>&1

echo "FAILURE LIST"
grep FAILURE cops_bin_push.tmp | sed 's/princeton_cops@//' | awk '{ print $4 }' | awk -F"." '{ print $2"\t"$1 }' | sort -u
rm cops_bin_push.tmp