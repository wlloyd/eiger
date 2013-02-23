package org.apache.cassandra.db.transaction;

public class YesVoteMessage extends CohortMessage
{
    public YesVoteMessage(long transactionId, int localKeyCount)
    {
        super(transactionId, localKeyCount);
    }

    @Override
    public int serializationFlags()
    {
        return TransactionMessageSerializer.YESVOTE_MASK;
    }
}
