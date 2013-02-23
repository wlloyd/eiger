package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DependencyCheckVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(DependencyCheckVerbHandler.class);

    @Override
    public void doVerb(Message message, String id)
    {
        DependencyCheck depCheck = null;
        try {
            depCheck = DependencyCheck.fromBytes(message.getMessageBody(), message.getVersion());
        } catch (IOException e) {
            logger_.error("Error in decoding dependencyCheck");
        }
        AppliedOperations.checkDependency(depCheck, message, id);
    }
}