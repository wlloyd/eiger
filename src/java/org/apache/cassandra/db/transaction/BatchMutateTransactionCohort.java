package org.apache.cassandra.db.transaction;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMutateTransactionCohort implements MessageProducer
{
//    INIT --receive data locally --> PREPARE
//    --receive data remotely --> --notify coordinator-->
//    WAIT_FOR_PREPARE --receive prepare-->
//    PREPARE --set lvt on involved keys--> --send yes-vote to coordinator-->
//    WAIT_FOR_COMMIT -- receive commit --apply batch mutate -- send ack to coordinator -- cleanup

    private static BatchMutateTransactionCohortSerializer serializer_ = new BatchMutateTransactionCohortSerializer();

    public static BatchMutateTransactionCohortSerializer serializer()
    {
        return serializer_;
    }

    private static enum CohortState {
        INIT,
        WAIT_FOR_PREPARE,
        PREPARE,
        WAIT_FOR_COMMIT,
        COMMITTED
    };

    private final Logger logger = LoggerFactory.getLogger(BatchMutateTransactionCohort.class);

    private static Map<Long, BatchMutateTransactionCohort> idToCohort = Collections.synchronizedMap(new HashMap<Long, BatchMutateTransactionCohort>());

    public static BatchMutateTransactionCohort findCohort(Long transactionId)
    {
        return idToCohort.get(transactionId);
    }

    private CohortState state;

    private String keyspace;
    private List<IMutation> mutations;
    private long transactionId;
    private ByteBuffer coordinatorKey;
    private int localKeyCount;

    private Long timestamp;
    private Long localCommitTime;
    private boolean local;

    // Ensure all functions are only called once
    private final boolean sanityCheckFireOnces = true; // also change in Coordinator
    private Boolean receivedTransaction = false;
    private Boolean receivedPrepare = false;
    private Boolean prepared = false;
    private Boolean receivedCommit = false;
    private Boolean cleanedUp = false;

    public BatchMutateTransactionCohort()
    {
        state = CohortState.INIT;
    }

    public void receiveLocalTransaction(String keyspace, Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ByteBuffer coordinator_key, long transaction_id)
    throws InvalidRequestException
    {
	if (sanityCheckFireOnces) {
	    synchronized(receivedTransaction) {
		assert receivedTransaction == false : "May only receive transaction " + transaction_id + " once";
		receivedTransaction = true;
	    }
	}

        assert state == CohortState.INIT : state + " != " + CohortState.INIT;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveLocalTransaction({}, {}, {}, {})", new Object[]{keyspace, mutation_map, coordinator_key, transaction_id});
        }

        List<IMutation> mutations = BatchMutateTransactionUtil.convertToInternalMutations(keyspace, mutation_map, coordinator_key);

        this.keyspace = keyspace;
        this.mutations = mutations;
        this.coordinatorKey = coordinator_key;
        this.transactionId = transaction_id;
        this.localKeyCount = mutations.size();
        this.local = true;

	// Register this cohort only after we're done constructing it
        idToCohort.put(transaction_id, this);
        BatchMutateTransactionUtil.registerCoordinatorKey(coordinator_key, transaction_id);

        state = CohortState.PREPARE;
        prepare();
    }

    public void receiveReplicatedTransaction(String keyspace, List<IMutation> mutations, ByteBuffer coordinator_key, long transaction_id, long timestamp)
    {
	if (sanityCheckFireOnces) {
	    synchronized(receivedTransaction) {
		assert receivedTransaction == false : "May only receive transaction " + transaction_id + " once";
		receivedTransaction = true;
	    }
	}

        assert state == CohortState.INIT : state + " != " + CohortState.INIT;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveReplicatedTransaction({}, {}, {}, {})", new Object[]{keyspace, mutations, ByteBufferUtil.bytesToHex(coordinator_key), transaction_id});
        }

        this.keyspace = keyspace;
        this.mutations = mutations;
        this.coordinatorKey = coordinator_key;
        this.transactionId = transaction_id;
        this.localKeyCount = mutations.size();
        this.timestamp = timestamp;
        this.local = false;

	// Register this cohort only after we're done constructing it
        BatchMutateTransactionUtil.registerCoordinatorKey(coordinator_key, transaction_id);
        idToCohort.put(transaction_id, this);

        state = CohortState.WAIT_FOR_PREPARE;
        try {
            TransactionProxy.sendNotify(keyspace, coordinatorKey, transactionId, localKeyCount);
        } catch (IOException e) {
	    e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void receivePrepare()
    {
	if (sanityCheckFireOnces) {
	    synchronized(receivedPrepare) {
		assert receivedPrepare == false : "May only receive Prepare for " + transactionId + " once";
		receivedPrepare = true;
	    }
	}

        assert state == CohortState.WAIT_FOR_PREPARE : state + " != " + CohortState.WAIT_FOR_PREPARE;

        if (logger.isTraceEnabled()) {
            logger.trace("receivePrepare() on {}", new Object[]{transactionId});
        }

        state = CohortState.PREPARE;
        prepare();
    }

    private void prepare()
    {
	if (sanityCheckFireOnces) {
	    synchronized(prepared) {
		assert prepared == false : "May prepare " + transactionId + " once";
		prepared = true;
	    }
	}

        assert state == CohortState.PREPARE : state + " != " + CohortState.PREPARE;

        if (logger.isTraceEnabled()) {
            logger.trace("prepare() on {}", new Object[]{transactionId});
        }

        //set latestValidTime on local keys
        try {
            BatchMutateTransactionUtil.markTransactionPending(keyspace, mutations, transactionId);
        } catch (Exception e) {
	    e.printStackTrace();
            throw new RuntimeException(e);
        }

        state = CohortState.WAIT_FOR_COMMIT;
        try {
            TransactionProxy.sendYesVote(keyspace, coordinatorKey, transactionId, localKeyCount);
        } catch (IOException e) {
	    e.printStackTrace();
            throw new RuntimeException(e);

        }
    }

    public void receiveCommit(long localCommitTime)
    {
	if (sanityCheckFireOnces) {
	    synchronized(receivedCommit) {
		assert receivedCommit == false : "May only receive Commit for " + transactionId + " once";
		receivedCommit = true;
	    }
	}

        assert state == CohortState.WAIT_FOR_COMMIT : state + " != " + CohortState.WAIT_FOR_COMMIT;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveCommit({}) on {}", new Object[]{localCommitTime, transactionId});
        }

        this.localCommitTime = localCommitTime;
        if (local) {
            this.timestamp = localCommitTime;
        }

        try {
            BatchMutateTransactionUtil.applyTransaction(keyspace, mutations, timestamp, localCommitTime, coordinatorKey);
        } catch (Exception e) {
	    e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            TransactionProxy.sendAck(keyspace, coordinatorKey, transactionId, localKeyCount);
        } catch (IOException e) {
	    e.printStackTrace();
            throw new RuntimeException(e);
        }

        if (local) {
            try {
                //WL TODO: have transaction proxy be aware of all keys so it can deal with datacenters with different key ranges
                ByteBuffer aLocalKey = mutations.iterator().next().key();
                TransactionProxy.replicateCohortToOtherDatacenters(this, keyspace, aLocalKey);
            } catch (IOException e) {
		e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        cleanup();
    }

    private void cleanup()
    {
	if (sanityCheckFireOnces) {
	    synchronized(cleanedUp) {
		assert cleanedUp == false : "May only cleanUp for " + transactionId + " once";
		cleanedUp = true;
	    }
	}

        BatchMutateTransactionUtil.unregisterCoordinatorKey(coordinatorKey, transactionId, localCommitTime);
        idToCohort.remove(transactionId);
    }

    private static class BatchMutateTransactionCohortSerializer implements IVersionedSerializer<BatchMutateTransactionCohort>
    {
        final static int ROW_MUTATION_FLAG = 0x01;
        final static int COUNTER_MUTATION_FLAG = 0x02;

        @Override
        public void serialize(BatchMutateTransactionCohort coordinator, DataOutput dos, int version) throws IOException
        {
            dos.writeUTF(coordinator.keyspace);

            dos.writeInt(coordinator.mutations.size());
            for (IMutation mutation : coordinator.mutations) {
                if (mutation instanceof RowMutation) {
                    dos.writeByte(ROW_MUTATION_FLAG);
                    RowMutation.serializer().serialize((RowMutation) mutation, dos, version);
                } else {
                    assert mutation instanceof CounterMutation;
                    dos.writeByte(COUNTER_MUTATION_FLAG);
                    CounterMutation.serializer().serialize((CounterMutation) mutation, dos, version);
                }
            }

            ByteBufferUtil.writeWithShortLength(coordinator.coordinatorKey, dos);

            dos.writeLong(coordinator.transactionId);

            dos.writeLong(coordinator.timestamp);
        }

        @Override
        public BatchMutateTransactionCohort deserialize(DataInput dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();

            int mutationsLength = dis.readInt();
            List<IMutation> mutations = new ArrayList<IMutation>(mutationsLength);
            for (int i = 0; i < mutationsLength; ++i) {

                int typeByte = dis.readUnsignedByte();
                if ((typeByte & ROW_MUTATION_FLAG) != 0) {
                    assert (typeByte & COUNTER_MUTATION_FLAG) == 0;

                    RowMutation rm = RowMutation.serializer().deserialize(dis, version);
                    mutations.add(rm);
                } else {
                    assert (typeByte & COUNTER_MUTATION_FLAG) != 0;
                    assert (typeByte & ROW_MUTATION_FLAG) == 0;

                    CounterMutation cm = CounterMutation.serializer().deserialize(dis, version);
                    mutations.add(cm);

                }
            }

            ByteBuffer coordinatorKey = ByteBufferUtil.readWithShortLength(dis);

            long transactionId = dis.readLong();

            long timestamp = dis.readLong();

            BatchMutateTransactionCohort cohort = new BatchMutateTransactionCohort();
            cohort.receiveReplicatedTransaction(keyspace, mutations, coordinatorKey, transactionId, timestamp);
            return cohort;
        }

        @Override
        public long serializedSize(BatchMutateTransactionCohort cohort, int version)
        {
            int size = 0;
            size += DBConstants.shortSize + FBUtilities.encodedUTF8Length(cohort.keyspace);
            size += DBConstants.intSize;
            for (IMutation mutation : cohort.mutations) {
                if (mutation instanceof RowMutation) {
                    size += 1;
                    size += RowMutation.serializer().serializedSize((RowMutation) mutation, version);
                } else {
                    assert mutation instanceof CounterMutation;
                    size += 1;
                    size += CounterMutation.serializer().serializedSize((CounterMutation) mutation, version);
                }
            }

            size += DBConstants.shortSize + cohort.coordinatorKey.remaining();

            size += DBConstants.longSize;

            size += DBConstants.longSize;

            return size;
        }
    }

    private final Map<Integer, byte[]> preserializedBuffers = new HashMap<Integer, byte[]>();

    public synchronized byte[] getSerializedBuffer(int version) throws IOException
    {
        byte[] bytes = preserializedBuffers.get(version);
        if (bytes == null)
        {
            bytes = FBUtilities.serialize(this, serializer(), version);
            preserializedBuffers.put(version, bytes);
        }
        return bytes;
    }

    public static BatchMutateTransactionCohort fromBytes(byte[] raw, int version) throws IOException
    {
        return serializer().deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }

    @Override
    public Message getMessage(Integer version) throws IOException
    {
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.TRANSACTION_COHORT, getSerializedBuffer(version), version);
    }
}
