package org.apache.cassandra.db;

import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;

public class RowMutationCompletion implements ICompletable
{
    private final Message message;
    private final String id;
    private final RowMutation rm;

    public RowMutationCompletion(Message message, String id, RowMutation rm)
    {
        this.message = message;
        this.id = id;
        this.rm = rm;
    }

    // Complete the blocked RowMutation
    @Override
    public void complete()
    {
        RowMutationVerbHandler.instance().applyAndRespond(message, id, rm);
    }

}
