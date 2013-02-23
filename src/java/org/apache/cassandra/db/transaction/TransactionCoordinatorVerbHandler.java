package org.apache.cassandra.db.transaction;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionCoordinatorVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(TransactionCoordinatorVerbHandler.class);

    @Override
    public void doVerb(Message message, String id)
    {
        try {
            BatchMutateTransactionCoordinator coordinator = BatchMutateTransactionCoordinator.fromBytes(message.getMessageBody(), message.getVersion());
            //deserialization calls receiveReplicated... on the coordinator
        } catch (IOException e) {
            logger_.error("Error in decoding batchMutateTransactionCoordinator");
        }

    }
}
