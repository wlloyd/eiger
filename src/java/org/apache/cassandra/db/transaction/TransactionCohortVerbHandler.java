package org.apache.cassandra.db.transaction;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionCohortVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(TransactionCohortVerbHandler.class);

    @Override
    public void doVerb(Message message, String id)
    {
        try {
            BatchMutateTransactionCohort cohort = BatchMutateTransactionCohort.fromBytes(message.getMessageBody(), message.getVersion());
            //deserialization calls receiveReplicated... on the cohort
        } catch (IOException e) {
            logger_.error("Error in decoding batchMutateTransactionCohort");
        }
    }
}
