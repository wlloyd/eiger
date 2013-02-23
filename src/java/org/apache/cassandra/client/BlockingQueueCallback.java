package org.apache.cassandra.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.thrift.async.AsyncMethodCallback;

public class BlockingQueueCallback<T> implements AsyncMethodCallback<T>
{
    public class ResponseOrException
    {
        private final T response;
        private final Exception exception;

        public ResponseOrException(T response)
        {
            this.response = response;
            this.exception = null;
        }

        public ResponseOrException(Exception exception)
        {
            this.response = null;
            this.exception = exception;
        }

        public T getResponse()
        {
            return response;
        }

        public Exception getException()
        {
            return exception;
        }

        public boolean isSetResponse()
        {
            return (response != null);
        }

        public boolean isSetException()
        {
            return (exception != null);
        }
    }

    private final BlockingQueue<ResponseOrException> responsesOrExceptions;

    public BlockingQueueCallback()
    {
        this.responsesOrExceptions = new LinkedBlockingQueue<ResponseOrException>();
    }

    public T getResponseNoInterruption() throws Exception
    {
        ResponseOrException responseOrException = null;
        while (true) {
            try {
                responseOrException = responsesOrExceptions.take();
                break;
            } catch (InterruptedException e) {
                //ignore, we want a response no matter what
                continue;
            }
        }
        if (responseOrException.isSetException()) {
            throw responseOrException.exception;
        } else {
            return responseOrException.response;
        }
    }

    @Override
    public void onComplete(T response)
    {
        try {
            responsesOrExceptions.put(new ResponseOrException(response));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assert false : "Should always be able to put to the response queue";
        }
    }

    @Override
    public void onError(Exception exception)
    {
        try {
            responsesOrExceptions.put(new ResponseOrException(exception));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assert false : "Should always be able to put to the exception queue";
        }
    }

    public T peekResponse()
    {
        if (this.responsesOrExceptions.size() == 0) {
            return null;
        } else {
            ResponseOrException roe = responsesOrExceptions.peek();
            return roe.response;
        }
    }
}
