package org.apache.cassandra.db.transaction;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTransactionMessage implements MessageProducer
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractTransactionMessage.class);

    public abstract int serializationFlags();

    private static TransactionMessageSerializer serializer_ = new TransactionMessageSerializer();

    public static TransactionMessageSerializer serializer()
    {
        return serializer_;
    }

    public static AbstractTransactionMessage fromBytes(byte[] raw, int version) throws IOException
    {
        return serializer().deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }

    public static class TransactionMessageSerializer implements IVersionedSerializer<AbstractTransactionMessage>
    {
        public final static int NOTIFY_MASK  = 0x01;
        public final static int YESVOTE_MASK = 0x02;
        public final static int ACK_MASK     = 0x04;
        public final static int PREPARE_MASK = 0x08;
        public final static int COMMIT_MASK  = 0x10;
        public final static int CHECK_MASK   = 0x20;
        public final static int CHECKED_MASK = 0x40;

        @Override
        public void serialize(AbstractTransactionMessage transactionMessage, DataOutput dos, int version) throws IOException
        {
            dos.writeByte(transactionMessage.serializationFlags());
            if (transactionMessage instanceof CohortMessage) {
                CohortMessage cohortMessage = (CohortMessage) transactionMessage;
                dos.writeLong(cohortMessage.transactionId);
                dos.writeInt(cohortMessage.localKeyCount);
            } else if (transactionMessage instanceof PrepareMessage) {
                PrepareMessage prepareMessage = (PrepareMessage) transactionMessage;
                dos.writeLong(prepareMessage.transactionId);
            } else if (transactionMessage instanceof CommitMessage) {
                CommitMessage commitMessage = (CommitMessage) transactionMessage;
                dos.writeLong(commitMessage.transactionId);
                dos.writeLong(commitMessage.commitTime);
            } else if (transactionMessage instanceof CheckTransactionMessage) {
                CheckTransactionMessage checkMessage = (CheckTransactionMessage) transactionMessage;
                dos.writeInt(checkMessage.transactionIds.size());
                for (Long transactionId : checkMessage.transactionIds) {
                    dos.writeLong(transactionId);
                }
                dos.writeLong(checkMessage.checkTime);
            } else if (transactionMessage instanceof TransactionsCheckedMessage) {
                TransactionsCheckedMessage checkedMessage = (TransactionsCheckedMessage) transactionMessage;
                dos.writeInt(checkedMessage.transactionIdToResult.size());
                for (Entry<Long,Long> idAndResult : checkedMessage.transactionIdToResult.entrySet()) {
                    dos.writeLong(idAndResult.getKey());
                    dos.writeLong(idAndResult.getValue());
                }
            } else {
                assert false : "Unknown type: " + transactionMessage.getClass();
            }
        }

        @Override
        public AbstractTransactionMessage deserialize(DataInput dis, int version) throws IOException
        {
            Byte typeByte = dis.readByte();
            if ((typeByte & NOTIFY_MASK) != 0) {
                long transactionId = dis.readLong();
                int localKeyCount = dis.readInt();
                return new NotifyMessage(transactionId, localKeyCount);
            } else if ((typeByte & YESVOTE_MASK) != 0) {
                long transactionId = dis.readLong();
                int localKeyCount = dis.readInt();
                return new YesVoteMessage(transactionId, localKeyCount);
            } else if ((typeByte & ACK_MASK) != 0) {
                long transactionId = dis.readLong();
                int localKeyCount = dis.readInt();
                return new AckMessage(transactionId, localKeyCount);
            } else if ((typeByte & PREPARE_MASK) != 0) {
                long transactionId = dis.readLong();
                return new PrepareMessage(transactionId);
            } else if ((typeByte & COMMIT_MASK) != 0) {
                long transactionId = dis.readLong();
                long commitTime = dis.readLong();
                return new CommitMessage(transactionId, commitTime);
            } else if ((typeByte & CHECK_MASK) != 0) {
                int numTransactionIds = dis.readInt();
                List<Long> transactionIds = new ArrayList<Long>(numTransactionIds);
                for (int i = 0; i < numTransactionIds; ++i) {
                    transactionIds.add(dis.readLong());
                }
                long chosenTime = dis.readLong();
                return new CheckTransactionMessage(transactionIds, chosenTime);
            } else if ((typeByte & CHECKED_MASK) != 0) {
                int numEntries = dis.readInt();
                Map<Long, Long> transactionIdToResult = new HashMap<Long, Long>(numEntries);
                for (int i = 0; i < numEntries; ++i) {
                    long transactionId = dis.readLong();
                    long commitTime = dis.readLong();
                    transactionIdToResult.put(transactionId, commitTime);
                }
                return new TransactionsCheckedMessage(transactionIdToResult);
            } else {
                assert false : "Unknown type: " + typeByte;
                return null;
            }
        }

        @Override
        public long serializedSize(AbstractTransactionMessage transactionMessage, int version)
        {
            if (transactionMessage instanceof CohortMessage) {
                return 1 + DBConstants.longSize + DBConstants.intSize;
            } else if (transactionMessage instanceof PrepareMessage) {
                return 1 + DBConstants.longSize;
            } else if (transactionMessage instanceof CommitMessage) {
                return 1 + DBConstants.longSize + DBConstants.longSize;
            } else if (transactionMessage instanceof CheckTransactionMessage) {
                return 1 + DBConstants.intSize + (((CheckTransactionMessage) transactionMessage).transactionIds.size() * DBConstants.longSize);
            } else if (transactionMessage instanceof TransactionsCheckedMessage) {
                return 1 + DBConstants.intSize + (((TransactionsCheckedMessage) transactionMessage).transactionIdToResult.size()) * (DBConstants.longSize + DBConstants.longSize);
            } else {
                assert false : "Unknown type: " + transactionMessage.getClass();
                return Long.MIN_VALUE;
            }
        }
    }

    @Override
    public Message getMessage(Integer version) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer().serialize(this, dob, version);

        logger.debug("getMessage called on " + this + " serialized into " + dob.getLength());

        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.TRANSACTION_MESSAGE, Arrays.copyOf(dob.getData(), dob.getLength()), version);
    }
}
