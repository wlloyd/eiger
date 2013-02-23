package org.apache.cassandra.service;

import java.util.Set;

import org.apache.cassandra.db.Dependency;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DepCheckCallback implements IAsyncCallback
{
    private static Logger logger_ = LoggerFactory.getLogger(DepCheckCallback.class);

    private final long startTime;
    private int responses = 0;
    private final Set<Dependency> deps;
    private final ICompletable completable;

    public DepCheckCallback(Set<Dependency> deps, ICompletable completable)
    {
        this.deps = deps;
        this.startTime = System.currentTimeMillis();
        this.completable = completable;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        //not on the read path
        return false;
    }

    @Override
    synchronized public void response(Message msg)
    {
        responses++;

        if (logger_.isDebugEnabled()) {
            logger_.debug("Response " + responses + "/" + deps.size() + " for " + completable);
        }

        assert responses > 0 && responses <= deps.size() : responses + "?" + deps.size();
        if (responses == deps.size()) {
            completable.complete();
        }
    }

    //WL TODO: Add a timeout that fires and complains if this hangs
}
