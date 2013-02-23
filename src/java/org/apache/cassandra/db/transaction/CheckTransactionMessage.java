package org.apache.cassandra.db.transaction;

import java.util.List;

public class CheckTransactionMessage extends AbstractTransactionMessage
{
    final List<Long> transactionIds;
    final long checkTime;

    public CheckTransactionMessage(List<Long> transactionIds, long chosenTime)
    {
        this.transactionIds = transactionIds;
        this.checkTime = chosenTime;
    }

    public List<Long> getTransactionIds()
    {
        return transactionIds;
    }

    public long getCheckTime()
    {
        return checkTime;
    }

    @Override
    public int serializationFlags()
    {
        return TransactionMessageSerializer.CHECK_MASK;
    }
}
