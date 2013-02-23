package org.apache.cassandra.stress.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ColumnOrSuperColumnHelper;

public class TwoRoundReadTxn extends Operation
{
    private static List<ByteBuffer> values;

    public TwoRoundReadTxn(Session session, int index)
    {
        super(session, index);
    }

    @Override
    public void run(Cassandra.Client client) throws IOException
    {
        throw new RuntimeException("TwoRoundReadTxn must be run with COPS client library");
    }

    @Override
    public void run(ClientLibrary clientLibrary) throws IOException
    {
        int columnsPerKey = session.getColumns_per_key_read();
        int keysPerRead = session.getKeys_per_read();

        //TODO: Perhaps we should randomize which columns we grab?
        SlicePredicate nColumnsPredicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      false, columnsPerKey));

        int offset = index * session.getKeysPerThread();
        Map<ByteBuffer,List<ColumnOrSuperColumn>> results;

        int columnCount = 0;
        long bytesCount = 0;

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            List<ByteBuffer> keys = generateKeys(offset, offset + keysPerRead);

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
                        results = clientLibrary.forced_2round_multiget_slice(keys, parent, nColumnsPredicate);
                        success = (results.size() > 0);
                        //success = (results.size() == columnsPerKey*keysPerRead);
                        if (!success)
                            exceptionMessage = "Wrong number of columns: " + results.size() + " instead of " + columnsPerKey*keysPerRead;

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

                offset += keysPerRead;
            }
        }
        else
        {
            ColumnParent parent = new ColumnParent("Standard1");

            List<ByteBuffer> keys = generateKeys(offset, offset + keysPerRead);

            long startNano = System.nanoTime();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    results = clientLibrary.forced_2round_multiget_slice(keys, parent, nColumnsPredicate);

                    success = (results.size() > 0);
                    //success = (results.size() == columnsPerKey*keysPerRead);
                    if (!success)
                        exceptionMessage = "Wrong number of columns: " + results.size() + " instead of " + columnsPerKey*keysPerRead;

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

            offset += keysPerRead;
        }
    }

    private List<ByteBuffer> generateKeys(int start, int limit)
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();

        for (int i = start; i < limit; i++)
        {
            keys.add(ByteBuffer.wrap(generateKey()));
        }

        return keys;
    }
}