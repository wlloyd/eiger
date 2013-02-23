package org.apache.cassandra.cops2;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.LamportClock;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.BeforeClass;

public class MultiDcCops2Test extends Cops2Test
{
    private static final int DEFAULT_THRIFT_PORT = 9160;

    private static void waitForKeyspacePropagation(Map<String, Integer> allServerIPAndPorts, String keyspace) throws TException
    {
        for (Entry<String, Integer> ipAndPort : allServerIPAndPorts.entrySet()) {
            String ip = ipAndPort.getKey();
            Integer port = ipAndPort.getValue();

            TTransport tFramedTransport = new TFramedTransport(new TSocket(ip, port));
            TProtocol binaryProtoOnFramed = new TBinaryProtocol(tFramedTransport);
            Cassandra.Client client = new Cassandra.Client(binaryProtoOnFramed);
            tFramedTransport.open();

            // FIXME: This is a hideous way to ensure the earlier system_add_keyspace has propagated everywhere
            while(true) {
                try {
                    client.set_keyspace(keyspace, LamportClock.sendTimestamp());
                    break;
                } catch (InvalidRequestException e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        //ignore
                    }
                }
            }
        }
    }

    @BeforeClass
    public static void setup() throws IOException, InterruptedException, ConfigurationException, InvalidRequestException, SchemaDisagreementException, TException
    {
        Integer numDatacenters = Integer.getInteger("cassandra.multiDcTest.numDatacenters");
        assert numDatacenters != null : "You must set the numDatacenters to run the multiDc Tests";
        Integer nodesPerDatacenter = Integer.getInteger("cassandra.multiDcTest.nodesPerDatacenter");
        assert nodesPerDatacenter != null : "You must set nodesPerDatacenter to run the multiDc Tests";


        //Create a keyspace with a replication factor of 1 for each datacenter
        TTransport tr = new TFramedTransport(new TSocket("127.0.0.1", DEFAULT_THRIFT_PORT));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        //set the replication factor to 1 for each datacenter
        Map<String, String> ntsOptions = new HashMap<String, String>();
        assert numDatacenters > 0;
        for (int i = 0; i < numDatacenters; ++i) {
            ntsOptions.put("DC"+i, "1");
        }

        //We'll use the same set of columns as is used in the EmbeddedCassandraService
        // and we'll set the index type to KEYS so thrift doesn't complain
        Map<String, CFMetaData> cfDefs = schemaDefinition().iterator().next().cfMetaData();
        for (Entry<String, CFMetaData> cfEntry : cfDefs.entrySet()) {
            assert cfEntry.getKey() == cfEntry.getValue().cfName;
            for (ColumnDefinition colDef : cfEntry.getValue().getColumn_metadata().values()) {
                colDef.setIndexType(IndexType.KEYS, null);
            }
            cfEntry.getValue().readRepairChance(0);
        }
        KSMetaData keyspace1 = KSMetaData.testMetadataNotDurable("Keyspace1", NetworkTopologyStrategy.class, ntsOptions, cfDefs.values());
        client.system_add_keyspace(keyspace1.toThrift());

        //setup the normal test
        HashMap<String, Integer> localServerIPAndPorts = new HashMap<String, Integer>();
        for (int i = 1; i <= nodesPerDatacenter; ++i) {
            localServerIPAndPorts.put("127.0.0." + i, DEFAULT_THRIFT_PORT);
        }
        List<Map<String, Integer>> dcToServerIPAndPorts = new ArrayList();
        for (int dc = 0; dc < numDatacenters; ++dc) {
            HashMap<String, Integer> serverIPAndPorts = new HashMap<String, Integer>();
            for (int i = 0; i < nodesPerDatacenter; ++i) {
                int ipIndex = 1 + dc*nodesPerDatacenter + i;
                serverIPAndPorts.put("127.0.0." + ipIndex, DEFAULT_THRIFT_PORT);
            }
            dcToServerIPAndPorts.add(serverIPAndPorts);
        }

        Cops2Test.setLocalServerIPAndPorts(localServerIPAndPorts);
        Cops2Test.setDcToServerIPAndPorts(dcToServerIPAndPorts);
        Cops2Test.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        //wait for the keyspace to show up at all nodes
        HashMap<String, Integer> allServerIPAndPorts = new HashMap<String, Integer>();
        for (int i = 1; i <= numDatacenters*nodesPerDatacenter; ++i) {
            allServerIPAndPorts.put("127.0.0." + i, DEFAULT_THRIFT_PORT);
        }
        waitForKeyspacePropagation(allServerIPAndPorts, "Keyspace1");
    }

    //Runs all the tests from Cops2Test
}
