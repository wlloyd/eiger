package org.apache.cassandra.db.transaction;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.SimpleCondition;

public class CheckTransactionCallback implements IAsyncCallback
{
    private final SimpleCondition condition = new SimpleCondition();
    private final long startTime;
    private final long checkedTime;

    public CheckTransactionCallback(long checkedTime)
    {
        this.checkedTime = checkedTime;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        //not on the read path
        return false;
    }

    @Override
    public void response(Message msg)
    {
        AbstractTransactionMessage abstractTransactionMessage;
        try {
            abstractTransactionMessage = AbstractTransactionMessage.fromBytes(msg.getMessageBody(), msg.getVersion());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TransactionsCheckedMessage checked = (TransactionsCheckedMessage) abstractTransactionMessage;
        BatchMutateTransactionUtil.updateCheckedTransaction(checked.getTransactionIdToResult(), checkedTime);
        condition.signal();
    }

    public void waitForResponse() throws TimeoutException
    {
        //WL TODO: If this timeout fires, we should warn and then keep trying
        long timeout = DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - startTime);
        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success) {
            throw new TimeoutException("CheckTransactions on " + checkedTime + " timed out");
        }
    }
}
