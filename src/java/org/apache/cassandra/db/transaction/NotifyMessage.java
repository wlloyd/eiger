package org.apache.cassandra.db.transaction;

public class NotifyMessage extends CohortMessage
{
    public NotifyMessage(long transactionId, int localKeyCount)
    {
        super(transactionId, localKeyCount);
    }

    @Override
    public int serializationFlags()
    {
        return TransactionMessageSerializer.NOTIFY_MASK;
    }
}
