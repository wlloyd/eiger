#!/bin/bash

if [ "$(uname -s)" == "Darwin" ]; then
    for pid in $(ps -Al | grep org.apache.cassandra.thrift.CassandraDaemon | grep -v grep | awk '{ print $2 }'); do
        kill -9 $pid;
    done
else
    for pid in $(ps -Afl | grep org.apache.cassandra.thrift.CassandraDaemon | grep -v grep | awk '{ print $4 }'); do
        kill -9 $pid;
    done
fi
