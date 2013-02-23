package org.apache.cassandra.cops2;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ShortNodeId;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;

public class EmbeddedCops2Test extends Cops2Test
{
    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException
    {
        EmbeddedCassandraService cassandra  = new EmbeddedCassandraService();
        ShortNodeId.updateShortNodeIds(Collections.singletonMap(InetAddress.getByName("127.0.0.1"), new String[] {"DC1:RAC1"}));
        cassandra.start();
        //setup the normal test
        HashMap<String, Integer> localServerIPAndPorts = new HashMap<String, Integer>();
        localServerIPAndPorts.put("localhost", DatabaseDescriptor.getRpcPort());
        Cops2Test.setLocalServerIPAndPorts(localServerIPAndPorts);
        Cops2Test.setConsistencyLevel(ConsistencyLevel.ONE);
    }

    //Runs all the tests from Cops2Test
}
