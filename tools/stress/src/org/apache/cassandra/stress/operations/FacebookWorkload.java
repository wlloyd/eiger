package org.apache.cassandra.stress.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ColumnOrSuperColumnHelper;
import org.apache.cassandra.utils.FBUtilities;

public class FacebookWorkload extends Operation
{
    private static List<ByteBuffer> values;
    private static ArrayList<Integer> columnCountList;

    public FacebookWorkload(Session session, int index)
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
        //do all random tosses here
        double opTypeToss = Stress.randomizer.nextDouble();
        if (opTypeToss <= .002) {
            double transactionToss = Stress.randomizer.nextDouble();
            boolean transaction = (transactionToss <= session.getWrite_transaction_fraction());

            //FB workload:
            //write N columns
            //write 1 key at a time
            //no write trans (i.e., write_trans_frac should be 0)
            write(clientLibrary, 1, transaction);
        } else {
            read(clientLibrary, getFBReadBatchSize());
        }
    }

    //This is a copy of MultiGetter.run with columnsPerKey and keysPerRead being used instead of the session parameters
    public void read(ClientLibrary clientLibrary, int keysPerRead) throws IOException
    {
        // We grab all columns for the key, they have been set there by the populator / writes
        // TODO ensure/make writes blow away all old columns
        SlicePredicate nColumnsPredicate = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .setReversed(false)
                .setCount(1024));


        Map<ByteBuffer,List<ColumnOrSuperColumn>> results;

        int columnCount = 0;
        long bytesCount = 0;

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            assert false : "Untested";

        List<ByteBuffer> keys = generateKeys(keysPerRead);

        for (int j = 0; j < session.getSuperColumns(); j++)
        {
            ColumnParent parent = new ColumnParent("Super1").setSuper_column(ByteBufferUtil.bytes("S" + j));

            long startNano = System.nanoTime();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    results = clientLibrary.transactional_multiget_slice(keys, parent, nColumnsPredicate);
                    success = (results.size() == keysPerRead);
                    if (!success)
                        exceptionMessage = "Wrong number of keys: " + results.size() + " instead of " + keysPerRead;

                    for (List<ColumnOrSuperColumn> result : results.values()) {
                        columnCount += result.size();
                        for (ColumnOrSuperColumn cosc : result) {
                            bytesCount += ColumnOrSuperColumnHelper.findLength(cosc);
                        }
                    }
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                }
            }

            if (!success)
            {
                error(String.format("Operation [%d] retried %d times - error on calling multiget_slice for keys %s %s%n",
                        index,
                        session.getRetryTimes(),
                        keys,
                        (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            session.operations.getAndIncrement();
            session.keys.getAndAdd(keys.size());
            session.columnCount.getAndAdd(columnCount);
            session.bytes.getAndAdd(bytesCount);
            long latencyNano = System.nanoTime() - startNano;
            session.latency.getAndAdd(latencyNano/1000000);
            session.latencies.add(latencyNano/1000);
        }
        }
        else
        {
            ColumnParent parent = new ColumnParent("Standard1");

            List<ByteBuffer> keys = generateKeys(keysPerRead);
            long startNano = System.nanoTime();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    columnCount = 0;
                    bytesCount = 0;

                    results = clientLibrary.transactional_multiget_slice(keys, parent, nColumnsPredicate);

                    success = (results.size() == keysPerRead);
                    if (!success)
                        exceptionMessage = "Wrong number of keys: " + results.size() + " instead of " + keysPerRead;

                    //String allReads = "Read ";
                    for (Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : results.entrySet())
                    {
                        ByteBuffer key = entry.getKey();
                        List<ColumnOrSuperColumn> columns = entry.getValue();

                        columnCount += columns.size();
                        success = (columns.size() > 0);
                        if (!success) {
                            exceptionMessage = "No columns returned for " + ByteBufferUtil.string(key);
                            break;
                        }

                        int keyByteTotal = 0;
                        for (ColumnOrSuperColumn cosc : columns) {
                            keyByteTotal += ColumnOrSuperColumnHelper.findLength(cosc);
                        }
                        bytesCount += keyByteTotal;

                        //allReads += ByteBufferUtil.string(key) + " = " + columns.size() + " cols = " + keyByteTotal + "B, ";
                    }
                    //allReads = allReads.substring(0, allReads.length() - 2);
                    //System.out.println(allReads);
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                    success = false;
                }
            }

            if (!success)
            {
                List<String> raw_keys = new ArrayList<String>();
                for (ByteBuffer key : keys) {
                    raw_keys.add(ByteBufferUtil.string(key));
                }
                error(String.format("Operation [%d] retried %d times - error on calling multiget_slice for keys %s %s%n",
                        index,
                        session.getRetryTimes(),
                        raw_keys,
                        (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            session.operations.getAndIncrement();
            session.keys.getAndAdd(keys.size());
            session.columnCount.getAndAdd(columnCount);
            session.bytes.getAndAdd(bytesCount);
            long latencyNano = System.nanoTime() - startNano;
            session.latency.getAndAdd(latencyNano/1000000);
            session.latencies.add(latencyNano/1000);
        }
    }

    private List<ByteBuffer> generateKeys(int numKeys) throws IOException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();

        for (int i = 0; i < numKeys*10 && keys.size() < numKeys; i++)
        {
            // We don't want to repeat keys within a mutate or a slice
            // TODO make more efficient
            ByteBuffer newKey = ByteBuffer.wrap(generateKey());
            if (!keys.contains(newKey)) {
                keys.add(newKey);
            }
        }

        if (keys.size() != numKeys) {
            error("Could not generate enough unique keys, " + keys.size() + " instead of " + numKeys);
        }

        return keys;
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


    public void write(ClientLibrary clientLibrary, int keysPerWrite, boolean transaction) throws IOException
    {
        if (values == null)
            values = generateFBValues();
        if (columnCountList == null)
            columnCountList = generateFBColumnCounts();

        Map<ByteBuffer, Map<String, List<Mutation>>> records = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        List<ByteBuffer> keys = generateKeys(keysPerWrite);
        int totalColumns = 0;
        int totalBytes = 0;
        for (ByteBuffer key : keys)
        {
            int numColumns = getFBColumnCount(key);
            totalColumns += numColumns;

            List<Column> columns = new ArrayList<Column>();
            int keyByteTotal = 0;
            for (int i = 0; i < numColumns; i++)
            {
                ByteBuffer value = getFBValue();
                keyByteTotal += value.limit() - value.position();

                columns.add(new Column(columnName(i, session.timeUUIDComparator))
                .setValue(value)
                .setTimestamp(FBUtilities.timestampMicros()));
            }
            totalBytes += keyByteTotal;

            assert session.getColumnFamilyType() != ColumnFamilyType.Super : "Unhandled";

            //System.out.println("Writing " + ByteBufferUtil.string(key) + " with " + numColumns + " columns and " + keyByteTotal + " bytes");
            records.put(key, getColumnsMutationMap(columns));
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
                if (transaction) {
                    clientLibrary.transactional_batch_mutate(records);
                } else {
                    clientLibrary.batch_mutate(records);
                }
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error inserting keys %s %s%n",
                    index,
                    session.getRetryTimes(),
                    keys,
                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndAdd(keysPerWrite);
        session.columnCount.getAndAdd(keysPerWrite*session.getColumns_per_key_write());
        session.bytes.getAndAdd(keysPerWrite*session.getColumns_per_key_write()*session.getColumnSize());
        long latencyNano = System.nanoTime() - startNano;
        session.latency.getAndAdd(latencyNano/1000000);
        session.latencies.add(latencyNano/1000);
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