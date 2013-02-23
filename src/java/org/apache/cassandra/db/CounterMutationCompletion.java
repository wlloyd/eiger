package org.apache.cassandra.db;

import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;

public class CounterMutationCompletion implements ICompletable
{
    private final Message message;
    private final String id;
    private final CounterMutation cm;

    public CounterMutationCompletion(Message message, String id, CounterMutation cm)
    {
        this.message = message;
        this.id = id;
        this.cm = cm;
    }

    // Complete the blocked RowMutation
    @Override
    public void complete()
    {
        CounterMutationVerbHandler.instance().applyAndRespond(message, id, cm);
    }

}
