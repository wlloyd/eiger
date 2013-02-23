#!/bin/bash

set -u

if [ $# -ne 1 ]; then
    echo "Usage: "$0" [dir with client0 ... in it]"
    exit
fi

output_dir=$1

#have client/system/trial ... want trial/system/client
for client_full in $(ls -d $output_dir/client*); do
    client=$(echo $client_full | awk -F"${output_dir}/" '{ print $2 }')
    echo $client
    for system_full in $(ls -dp ${client_full}/* | grep '/$'); do
	system=$(echo $system_full | awk -F"${client_full}/" '{ print $2 }')
	echo "  "$system
	for trial_full in $(ls -dp ${system_full}/* | grep '/$'); do
	    trial=$(echo $trial_full | awk -F"${system_full}/" '{ print $2 }')
	    echo "    "$trial

	    target_dir="$output_dir/$trial/$system/$client"
	    mkdir -p $target_dir
	    cp $trial_full/* $target_dir/
	done
    done
done

cd $output_dir
tar -cjf clis.tar.bz2 client*
mkdir .trash
mv client* .trash