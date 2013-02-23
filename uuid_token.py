#!/usr/bin/python

import sys

#From http://wiki.apache.org/cassandra/ByteOrderedPartitioner
#modified to include dc_index to keep tokens unique

def get_cassandra_tokens_uuid4_keys_bop(node_count, dc_index):
    # BOP expects tokens to be byte arrays, specified in hex
    return ["%032x" % (i*(2**128)/node_count + dc_index) for i in xrange(0, node_count)]

if len(sys.argv) != 4:
    print "Usage: " + sys.argv[0] + " [dc index] [node index] [nodes/dc]"
    sys.exit(-1)

dc_index = int(sys.argv[1])
node_index = int(sys.argv[2])
nodes_per_dc = int(sys.argv[3])

print get_cassandra_tokens_uuid4_keys_bop(nodes_per_dc, dc_index)[node_index]
