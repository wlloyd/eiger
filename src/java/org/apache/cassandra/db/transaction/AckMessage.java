package org.apache.cassandra.db.transaction;

public class AckMessage extends CohortMessage
{
    public AckMessage(long transactionId, int localKeyCount)
    {
        super(transactionId, localKeyCount);
    }

    @Override
    public int serializationFlags()
    {
        return TransactionMessageSerializer.ACK_MASK;
    }
}
