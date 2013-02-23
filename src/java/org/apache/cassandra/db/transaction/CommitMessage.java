package org.apache.cassandra.db.transaction;


public class CommitMessage extends AbstractTransactionMessage
{
    final long transactionId;
    final long commitTime;

    public CommitMessage(long transactionId, long commitTime)
    {
        this.transactionId = transactionId;
        this.commitTime = commitTime;
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public long getCommitTime()
    {
        return commitTime;
    }

    @Override
    public int serializationFlags()
    {
        return TransactionMessageSerializer.COMMIT_MASK;
    }
}
