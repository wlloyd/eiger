package org.apache.cassandra.db.transaction;


public class PrepareMessage extends AbstractTransactionMessage
{
    final long transactionId;

    public PrepareMessage(long transactionId)
    {
        this.transactionId = transactionId;
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    @Override
    public int serializationFlags()
    {
        return TransactionMessageSerializer.PREPARE_MASK;
    }
}
