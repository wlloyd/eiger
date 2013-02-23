package org.apache.cassandra.db.transaction;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;

public class TransactionsCheckedMessage extends AbstractTransactionMessage
{
    final Map<Long, Long> transactionIdToResult;

    public TransactionsCheckedMessage(Map<Long, Long> map)
    {
        this.transactionIdToResult = map;
    }

    public Map<Long, Long> getTransactionIdToResult()
    {
        return transactionIdToResult;
    }

    @Override
    public int serializationFlags()
    {
        return TransactionMessageSerializer.CHECKED_MASK;
    }

    public Message getReply(Message originalMessage) throws IOException
    {
        int size = DBConstants.intSize + transactionIdToResult.size() * (DBConstants.longSize + DBConstants.longSize);

        DataOutputBuffer buffer = new DataOutputBuffer(size);
        buffer.writeInt(transactionIdToResult.size());
        for (Entry<Long, Long> idAndResult : transactionIdToResult.entrySet()) {
            buffer.writeLong(idAndResult.getKey());
            buffer.writeLong(idAndResult.getValue());
        }
        return originalMessage.getReply(FBUtilities.getBroadcastAddress(), buffer.getData(), originalMessage.getVersion());
    }
}
