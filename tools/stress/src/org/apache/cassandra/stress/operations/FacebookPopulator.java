package org.apache.cassandra.stress.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class FacebookPopulator extends Operation
{
    private static ArrayList<ByteBuffer> values;
    private static ArrayList<Integer> columnCountList;

    public FacebookPopulator(Session session, int index)
    {
        super(session, index);
    }

    @Override
    public void run(Cassandra.Client client) throws IOException
    {
        throw new RuntimeException("Dynamic Workload must be run with COPS client library");
    }

    @Override
    public void run(ClientLibrary clientLibrary) throws IOException
    {
        if (values == null)
            values = generateFBValues();
        if (columnCountList == null)
            columnCountList = generateFBColumnCounts();

        // format used for keys
        String format = "%0" + session.getTotalKeysLength() + "d";
        String rawKey = String.format(format, index);
        ByteBuffer key = ByteBufferUtil.bytes(rawKey);

        List<Column> columns = new ArrayList<Column>();
        int columnCount = getFBColumnCount(key);

        int totalBytes = 0;
        for (int i = 0; i < columnCount; i++)
        {
            ByteBuffer value = getFBValue();
            totalBytes += value.limit() - value.position();
            columns.add(new Column(columnName(i, session.timeUUIDComparator))
            .setValue(value)
            .setTimestamp(FBUtilities.timestampMicros()));
        }

        //System.out.println("Populating " + rawKey + " with " + columnCount + " columns" + " and " + totalBytes + " bytes");
        Map<ByteBuffer, Map<String, List<Mutation>>> records = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        records.put(key, getColumnsMutationMap(columns));

        long start = System.currentTimeMillis();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                clientLibrary.batch_mutate(records);
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                if (t + 1 == session.getRetryTimes()) {
                    e.printStackTrace();
                }
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error inserting keys %s %s%n",
                    index,
                    session.getRetryTimes(),
                    rawKey,
                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndAdd(1);
        session.columnCount.getAndAdd(1*columnCount);
        session.bytes.getAndAdd(totalBytes);
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }

    private ByteBuffer getFBValue()
    {
        return values.get(Stress.randomizer.nextInt(values.size()));
    }

    private ArrayList<Integer> generateFBColumnCounts()
    {
        Random randomizer = new Random();
        randomizer.setSeed(0);

        ArrayList<Integer> columnCountList = new ArrayList<Integer>();
        for (int i = 0; i < session.getNumTotalKeys(); i++)
        {
            columnCountList.add(getFBColumnCount(randomizer));
        }

        return columnCountList;
    }

    protected int getFBColumnCount(ByteBuffer key) throws IOException
    {
        return columnCountList.get(Integer.parseInt(ByteBufferUtil.string(key)));
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