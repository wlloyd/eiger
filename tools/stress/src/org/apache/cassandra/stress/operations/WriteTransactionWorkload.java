package org.apache.cassandra.stress.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class WriteTransactionWorkload extends Operation
{
    private static List<ByteBuffer> values;

    private final boolean transactional;

    public WriteTransactionWorkload(Session session, int index, boolean transactional)
    {
        super(session, index);
        this.transactional = transactional;
    }

    @Override
    public void run(Cassandra.Client client) throws IOException
    {
        throw new RuntimeException("Write Transaction Workload must be run with COPS client library");
    }

    @Override
    public void run(ClientLibrary clientLibrary) throws IOException
    {
        int columnsPerKey = session.getColumns_per_key_write();

        if (values == null)
            values = generateValues();

        List<Column> columns = new ArrayList<Column>();
        List<SuperColumn> superColumns = new ArrayList<SuperColumn>();

        for (int i = 0; i < columnsPerKey; i++)
        {
            columns.add(new Column(columnName(i, session.timeUUIDComparator))
                                .setValue(values.get(i % values.size()))
                                .setTimestamp(FBUtilities.timestampMicros()));
        }

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            // supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                superColumns.add(new SuperColumn(ByteBufferUtil.bytes(superColumnName), columns));
            }
        }

        Map<ByteBuffer, Map<String, List<Mutation>>> records = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        List<ByteBuffer> keys = generateWriteTxnKeys(session.getNum_servers(), session.getServers_per_txn(), session.getKeys_per_server());
        for (ByteBuffer key : keys)
        {
            records.put(key, session.getColumnFamilyType() == ColumnFamilyType.Super
                             ? getSuperColumnsMutationMap(superColumns)
                             : getColumnsMutationMap(columns));
        }

        long startNano = System.nanoTime();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                if (transactional) {
                    clientLibrary.transactional_batch_mutate(records);
                } else {
                    clientLibrary.batch_mutate(records);
                }
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                if (t == session.getRetryTimes() - 1) {
                    e.printStackTrace();
                }
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error (stack trace above) inserting keys %s %s%n",
                                index,
                                session.getRetryTimes(),
                                keys,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndAdd(session.getServers_per_txn()*session.getKeys_per_server());
        session.columnCount.getAndAdd(session.getServers_per_txn()*session.getKeys_per_server()*session.getColumns_per_key_write());
        session.bytes.getAndAdd(session.getServers_per_txn()*session.getKeys_per_server()*session.getColumns_per_key_write()*session.getColumnSize());
        long latencyNano = System.nanoTime() - startNano;
        session.latency.getAndAdd(latencyNano/1000000);
        session.latencies.add(latencyNano/1000);
    }


    private List<ByteBuffer> generateWriteTxnKeys(int numTotalServers, int numInvolvedServers, int keysPerServer)
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();

        List<Integer> allServerIndices = new ArrayList<Integer>(numTotalServers);
        for (int i = 0; i < numTotalServers; i++)
        {
            allServerIndices.add(i);
        }

        // choose the involved serves, ensuring they are different
        List<Integer> involvedServers = new ArrayList<Integer>(numInvolvedServers);
        for (int i = 0; i < numInvolvedServers; i++)
        {
            int chosenServer = allServerIndices.remove(Stress.randomizer.nextInt(numTotalServers - i));
            involvedServers.add(chosenServer);
        }

        // choose K keys for each server
        for (int i = 0; i < involvedServers.size(); i++)
        {
	    int srvIndex = involvedServers.get(i);
            for (int k = 0; k < keysPerServer; k++)
            {
                keys.add(session.getRandGeneratedKey(srvIndex));
            }
        }

        return keys;
    }


    private Map<String, List<Mutation>> getSuperColumnsMutationMap(List<SuperColumn> superColumns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (SuperColumn s : superColumns)
        {
            ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn().setSuper_column(s);
            mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
        }

        mutationMap.put("Super1", mutations);

        return mutationMap;
    }

    private Map<String, List<Mutation>> getColumnsMutationMap(List<Column> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }

        mutationMap.put("Standard1", mutations);

        return mutationMap;
    }
}