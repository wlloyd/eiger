package org.apache.cassandra.db.transaction;

public abstract class CohortMessage extends AbstractTransactionMessage
{
    final long transactionId;
    final int localKeyCount;

    public CohortMessage(long transactionId, int localKeyCount)
    {
        this.transactionId = transactionId;
        this.localKeyCount = localKeyCount;
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public int getLocalKeyCount()
    {
        return localKeyCount;
    }
}
