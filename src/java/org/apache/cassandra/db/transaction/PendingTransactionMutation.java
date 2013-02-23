package org.apache.cassandra.db.transaction;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;

public class PendingTransactionMutation extends RowMutation
{
    private final long pendingTime;
    private final long transactionId;

    public PendingTransactionMutation(String table, ByteBuffer key, long pendingTime, long transactionId)
    {
        super(table, key);
        this.pendingTime = pendingTime;
        this.transactionId = transactionId;
    }

    @Override
    public boolean isEmpty()
    {
        return modifications_.isEmpty();
    }

    /* To mark columns as in a pending transaction, we'll apply the normal
     * mutation except we'll use PendingTransactionColumns instead of what's
     * ultimately intended
     */
    public void makePending(QueryPath path)
    {
        Integer id = Schema.instance.getId(table_, path.columnFamilyName);
        ColumnFamily columnFamily = modifications_.get(id);
        if (columnFamily == null)
        {
            columnFamily = ColumnFamily.create(table_, path.columnFamilyName);
            modifications_.put(id, columnFamily);
        }
        columnFamily.addPendingTransactionColumn(path, transactionId, pendingTime);
    }

    @Override
    public void add(QueryPath path, ByteBuffer value, long timestamp, long earliestValidTime, int timeToLive, ByteBuffer transactionCoordinatorKey) {
        makePending(path);
    }

    @Override
    public void addCounter(QueryPath path, long value, long timestamp, long earliestValidTime, ByteBuffer transactionCoordinatorKey) {
        makePending(path);
    }

    @Override
    public void delete(QueryPath path, long timestamp, long earliestValidTime, ByteBuffer transactionCoordinatorKey) {
        makePending(path);
    }


    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
     */
    @Override
    public void apply() throws IOException
    {
        KSMetaData ksm = Schema.instance.getTableDefinition(getTable());

        Table.open(table_).apply(this, ksm.durableWrites);
    }

    @Override
    public void applyUnsafe() throws IOException
    {
        Table.open(table_).apply(this, false);
    }
}
