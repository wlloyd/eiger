package org.apache.cassandra.client;

import org.apache.thrift.async.AsyncMethodCallback;

public class IgnoredCallback<T> implements AsyncMethodCallback<T>
{
    public IgnoredCallback() {}

    @Override
    public void onComplete(T response)
    {
        //ignored
    }

    @Override
    public void onError(Exception exception)
    {
        assert false : "Should not be an exception on oneway calls: " + exception;
    }
}
