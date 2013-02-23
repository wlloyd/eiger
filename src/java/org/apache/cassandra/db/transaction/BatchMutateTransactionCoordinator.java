package org.apache.cassandra.db.transaction;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LamportClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMutateTransactionCoordinator implements MessageProducer
{
    //    coordinator:
    //        INIT --receive data locally --> WAIT_FOR_PREPARES
    //             --receive data remotely --> --send out dep_checks-->
    //        WAIT_FOR_DEPS_AND_NOTIFICATIONS --> receive dep_check_response --> self
    //                                        --> receive notify --> self
    //                                        --> receive last notify and last dep_check_response -->
    //        SEND_PREPARES -- send prepare to all cohorts -->
    //        WAIT_FOR_VOTES -- receive all yes-votes -->
    //        COMMIT --set lvt on local keys --> choose commit time --> commit locally --> send commit to cohorts -->
    //        WAIT_FOR_ACKS -- receive all acks --> cleanup local state

    private static BatchMutateTransactionCoordinatorSerializer serializer_ = new BatchMutateTransactionCoordinatorSerializer();

    public static BatchMutateTransactionCoordinatorSerializer serializer()
    {
        return serializer_;
    }

    private static enum CoordinatorState {
        INIT,
        WAIT_FOR_DEPS_AND_NOTIFICATIONS,
        SEND_PREPARES,
        WAIT_FOR_VOTES,
        COMMIT,
        WAIT_FOR_ACKS
    };

    private static final Logger static_logger = LoggerFactory.getLogger(BatchMutateTransactionCoordinator.class);
    private final Logger logger = LoggerFactory.getLogger(BatchMutateTransactionCoordinator.class);

    // TODO: Switch to ConcurrentHashMap?
    private static Map<Long, BatchMutateTransactionCoordinator> idToCoordinator = Collections.synchronizedMap(new HashMap<Long, BatchMutateTransactionCoordinator>());
    private static Map<Long, Integer> queuedNotifications = new HashMap<Long, Integer>();
    private static Map<Long, Integer> queuedYesVotes = new HashMap<Long, Integer>();

    public static BatchMutateTransactionCoordinator findCoordinator(Long transactionId)
    {
        return idToCoordinator.get(transactionId);
    }

    /** Delivers the notifications to a coordinator if it exists, and otherwise queues it up for later delivery
     *
     * @param transactionId
     * @param notificationCount
     */
    public static void deliverNotification(Long transactionId, int notificationCount)
    {
        if (static_logger.isTraceEnabled()) {
            static_logger.trace("deliverNotification({}, {})", new Object[]{transactionId, notificationCount});
        }

        // Notification delivery & coordinator finding are synchronized together to avoid a race between:
        //  findCoordinator then queue notification and
        //  create coordinator then get past where we check for queued notifications
        // If a coordinator is created after we have the lock, it must
        // wait for us to release the lock before it can check for
        // queued notifications and is therefor guarenteed to see them
        BatchMutateTransactionCoordinator coordinator = null;
        synchronized(queuedNotifications) {
            coordinator = findCoordinator(transactionId);
            if (coordinator == null) {
                Integer previousCount = queuedNotifications.get(transactionId);
                if (previousCount != null) {
                    queuedNotifications.put(transactionId, previousCount + notificationCount);
                } else {
                    queuedNotifications.put(transactionId, notificationCount);
                }
            }
        }
        // Send the notify outside the synchronized block so other
        // notifications can be delivered to other coordinators
        if (coordinator != null) {
            coordinator.receiveNotify(notificationCount);
        }
    }

    /** Delivers the yesVotes to a coordinator if it exists, and otherwise queues it up for later delivery
     *
     * @param transactionId
     * @param yesVoteCount
     */
    public static void deliverYesVotes(Long transactionId, int yesVoteCount)
    {
        if (static_logger.isTraceEnabled()) {
            static_logger.trace("deliverYesVotes({}, {})", new Object[]{transactionId, yesVoteCount});
        }

        // Yesvote delivery & coordinator finding are synchronized together to avoid a race
        //  See deliverNotify for reasoning as why this is correct
        BatchMutateTransactionCoordinator coordinator = null;
        synchronized(queuedYesVotes) {
            coordinator = findCoordinator(transactionId);
            if (coordinator == null) {
                Integer previousCount = queuedYesVotes.get(transactionId);
                if (previousCount != null) {
                    queuedYesVotes.put(transactionId, previousCount + yesVoteCount);
                } else {
                    queuedYesVotes.put(transactionId, yesVoteCount);
                }
            }
        }
        if (coordinator != null) {
            coordinator.receiveYesVotes(yesVoteCount);
        }
    }


    private CoordinatorState state;
    private Integer yesVotesReceived = 0;
    private Integer acksReceived = 0;
    private Integer notifiesReceived = 0;

    private String keyspace;
    private List<IMutation> mutations;
    private Set<Dependency> deps;
    private Set<ByteBuffer> allKeys;
    private List<ByteBuffer> remoteKeys;
    private long transactionId;
    private ByteBuffer coordinatorKey;
    private LinkedBlockingQueue<Long> completionBlockingQueue;
    private boolean local;
    private boolean depChecksReturned;

    private Long timestamp; //localCommitTime in accepting datacenter
    private Long localCommitTime;

    private final int waitForLocalCommitTimeoutSecs = 5;

    // Ensure non-message-receiving functions are only called once
    private final boolean sanityCheckFireOnces = true; // also change in Cohort
    private Boolean receivedTransaction = false;
    private Boolean checkedQueuedNotifications = false;
    private Boolean sentPrepares = false;
    private Boolean checkedQueuedYesVotes = false;
    private Boolean committed = false;
    private Boolean cleanedUp = false;


    public BatchMutateTransactionCoordinator()
    {
        if (logger.isTraceEnabled()) {
            logger.trace("BatchMutateTransactionCoordinator()", new Object[]{});
        }
        state = CoordinatorState.INIT;
    }

    private void receiveTransaction(String keyspace, List<IMutation> mutations, Set<Dependency> deps, ByteBuffer coordinator_key, Set<ByteBuffer> all_keys, long transaction_id)
    {
        if (sanityCheckFireOnces) {
            synchronized(receivedTransaction) {
                assert receivedTransaction == false : "May only receive transaction " + transaction_id + " once";
                receivedTransaction = true;
            }
        }

        this.keyspace = keyspace;
        this.mutations = mutations;
        this.deps = deps;
        this.allKeys = all_keys;
        this.transactionId = transaction_id;
        this.coordinatorKey = coordinator_key;
        this.depChecksReturned = false;

        Set<ByteBuffer> localKeys = new HashSet<ByteBuffer>();
        for (IMutation mutation : mutations) {
            localKeys.add(mutation.key());
        }

        remoteKeys = new ArrayList<ByteBuffer>(allKeys.size());
        for (ByteBuffer key : allKeys) {
            if (!localKeys.contains(key)) {
                remoteKeys.add(key);
            }
        }
    }

    public void receiveLocalTransaction(String keyspace, Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, Set<Dependency> deps, ByteBuffer coordinator_key, Set<ByteBuffer> all_keys, long transaction_id)
            throws InvalidRequestException, IOException, TimeoutException
    {
        assert state == CoordinatorState.INIT : state + " != " + CoordinatorState.INIT + " on " + transaction_id;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveLocalTransaction({}, {}, {}, {}, {}, {})", new Object[]{keyspace, mutation_map, deps, coordinator_key, all_keys, transaction_id});
        }

        List<IMutation> mutations = BatchMutateTransactionUtil.convertToInternalMutations(keyspace, mutation_map, coordinator_key);

        receiveTransaction(keyspace, mutations, deps, coordinator_key, all_keys, transaction_id);
        this.completionBlockingQueue = new LinkedBlockingQueue<Long>();
        this.local = true;

        // Register this coordinator only after we're done constructing it
        idToCoordinator.put(transactionId, this);
        BatchMutateTransactionUtil.registerCoordinatorKey(coordinator_key, transaction_id);

        state = CoordinatorState.WAIT_FOR_VOTES;
        checkQueuedYesVotes();
    }

    public void receiveReplicatedTransaction(String keyspace, List<IMutation> mutations, Set<Dependency> deps, ByteBuffer coordinator_key, Set<ByteBuffer> all_keys, long transaction_id, long timestamp)
    {
        assert state == CoordinatorState.INIT : state + " != " + CoordinatorState.INIT + " on " + transaction_id;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveReplicatedTransaction({}, {}, {}, {}, {}, {}, {})", new Object[]{keyspace, mutations, deps, coordinator_key, all_keys, transaction_id, timestamp});
        }

        receiveTransaction(keyspace, mutations, deps, coordinator_key, all_keys, transaction_id);
        this.timestamp = timestamp;
        this.completionBlockingQueue = null;
        this.local = false;

        //mark as pending for depChecks
        AppliedOperations.addPendingOp(coordinator_key, timestamp);

        // Register this coordinator only after we're done constructing it
        idToCoordinator.put(transactionId, this);
        BatchMutateTransactionUtil.registerCoordinatorKey(coordinator_key, transaction_id);

        state = CoordinatorState.WAIT_FOR_DEPS_AND_NOTIFICATIONS;
        StorageProxy.checkDependencies(keyspace, coordinatorKey, timestamp, deps, new TransactionDepCheckCompletable());
        checkQueuedNotifications();
    }

    private class TransactionDepCheckCompletable implements ICompletable
    {
        @Override
        public void complete()
        {
            depChecksReturned = true;
            receiveNotify(0);
        }
    }

    private void checkQueuedNotifications()
    {
        if (sanityCheckFireOnces) {
            synchronized(checkedQueuedNotifications) {
                assert checkedQueuedNotifications == false : "May only checkQueuedNotifications for " + transactionId + " once";
                checkedQueuedNotifications = true;
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("checkQueuedNotifications() on {}", new Object[]{transactionId});
        }

        Integer queuedCount;
        synchronized(queuedNotifications) {
            queuedCount = queuedNotifications.get(transactionId);
            if (queuedCount != null) {
                queuedNotifications.remove(transactionId);
            }
        }

        //call receiveNotify even if none are queued in case all notification came in when we were in the INIT state
        if (queuedCount == null) {
            receiveNotify(0);
        } else {
            receiveNotify(queuedCount);
        }
    }

    public synchronized void receiveNotify(int keysNotified)
    {
        CoordinatorState stateWhenInvoked = state;
        //assert stateWhenInvoked == CoordinatorState.INIT || stateWhenInvoked == CoordinatorState.WAIT_FOR_DEPS_AND_NOTIFICATIONS : stateWhenInvoked + " on " + transactionId;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveNotify({} out of {} - {}) on {} @ ", new Object[]{keysNotified, remoteKeys.size(), notifiesReceived, transactionId, stateWhenInvoked});
        }

        boolean shouldSendPrepares = false;
        synchronized(notifiesReceived) {
            notifiesReceived += keysNotified;
            assert notifiesReceived <= remoteKeys.size() : notifiesReceived + " ? " + remoteKeys.size() + " on "  + transactionId;

            if (notifiesReceived == remoteKeys.size() && stateWhenInvoked == CoordinatorState.WAIT_FOR_DEPS_AND_NOTIFICATIONS) {
                //defer sending prepares unless we release notifiesReceived
                shouldSendPrepares = true;
            }
        }

        if (shouldSendPrepares) {
            state = CoordinatorState.SEND_PREPARES;
            sendPrepares();
        }
    }

    private void sendPrepares()
    {
        if (sanityCheckFireOnces) {
            synchronized(sentPrepares) {
                assert sentPrepares == false : "May only sendPrepares for " + transactionId + " once";
                sentPrepares = true;
            }
        }

        assert state == CoordinatorState.SEND_PREPARES : state + " != " + CoordinatorState.SEND_PREPARES + " on " + transactionId;

        if (logger.isTraceEnabled()) {
            logger.trace("sendPrepares() on {}", new Object[]{transactionId});
        }

        state = CoordinatorState.WAIT_FOR_VOTES;
        try {
            TransactionProxy.sendPrepares(keyspace, remoteKeys, transactionId);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // In case there are no cohorts, we receive 0 votes immediately
        if (remoteKeys.size() == 0) {
            receiveYesVotes(0);
        }
    }


    /* only needs to be called for local transactions, replicated transactions
     * will always have a coordinator created and go through the first round
     * before yesVotes start coming in
     */
    private void checkQueuedYesVotes()
    {
        if (sanityCheckFireOnces) {
            synchronized(checkedQueuedYesVotes) {
                assert checkedQueuedYesVotes == false : "May only checkQueuedYesVotes for " + transactionId + " once";
                checkedQueuedYesVotes = true;
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("checkQueuedYesVotes() on {}", new Object[]{transactionId});
        }

        Integer queuedCount;
        synchronized(queuedYesVotes) {
            queuedCount = queuedYesVotes.get(transactionId);
            if (queuedCount != null) {
                queuedYesVotes.remove(transactionId);
            }
        }

        if (queuedCount == null) {
            queuedCount = 0;
        }
        receiveYesVotes(queuedCount);
    }

    public synchronized void receiveYesVotes(int keysPrepared)
    {
        CoordinatorState stateWhenInvoked = state;
        if (keysPrepared == 0 && stateWhenInvoked != CoordinatorState.WAIT_FOR_VOTES) {
            return;
        }
        assert stateWhenInvoked == CoordinatorState.WAIT_FOR_VOTES || stateWhenInvoked  == CoordinatorState.INIT : stateWhenInvoked + " != " + CoordinatorState.WAIT_FOR_VOTES + " on " + transactionId;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveYesVotes({} out of {} - {}) on {} @ {}", new Object[]{keysPrepared, remoteKeys.size(), yesVotesReceived, transactionId, stateWhenInvoked});
        }

        yesVotesReceived += keysPrepared;
        assert yesVotesReceived <= remoteKeys.size() : yesVotesReceived + " ? " + remoteKeys.size() + " on " + transactionId;

        if (yesVotesReceived == remoteKeys.size() && stateWhenInvoked == CoordinatorState.WAIT_FOR_VOTES) {
            state = CoordinatorState.COMMIT;
            commit();
        }
    }

    private void commit()
    {
        if (sanityCheckFireOnces) {
            synchronized(committed) {
                assert committed == false : "May only commit " + transactionId + " once";
                committed = true;
            }
        }

        assert state == CoordinatorState.COMMIT : state + " != " + CoordinatorState.COMMIT + " on " + transactionId;

        if (logger.isTraceEnabled()) {
            logger.trace("commit() on {}", new Object[]{transactionId});
        }

        //set latestValidTime on local keys
        //NOTE: Need to set a "don't" update flag here
        try {
            BatchMutateTransactionUtil.markTransactionPending(keyspace, mutations, transactionId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        //commit now
        localCommitTime  = LamportClock.getVersion();
        if (local) {
            timestamp = localCommitTime;
        }

        //send commit to cohorts
        try {
            TransactionProxy.sendCommit(keyspace, remoteKeys, transactionId, localCommitTime);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        //apply locally
        try {
            BatchMutateTransactionUtil.applyTransaction(keyspace, mutations, timestamp, localCommitTime, coordinatorKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        //mark as applied for depChecks
        AppliedOperations.addAppliedOp(coordinatorKey, timestamp);

        if (local) {
            try {
                TransactionProxy.replicateCoordinatorToOtherDatacenters(this, keyspace, coordinatorKey);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if (local) {
            completionBlockingQueue.add(timestamp);
        }

        state = CoordinatorState.WAIT_FOR_ACKS;
        receiveAck(0);
    }

    public synchronized void receiveAck(int keysAcked)
    {
        CoordinatorState stateWhenInvoked = state;
        // null state is okay if this is this is called with (0) and a previously received ack caused the cleanup
        assert stateWhenInvoked == CoordinatorState.WAIT_FOR_ACKS || stateWhenInvoked == CoordinatorState.COMMIT || stateWhenInvoked == null : stateWhenInvoked + " on " + transactionId;

        if (logger.isTraceEnabled()) {
            logger.trace("receiveAck({} out of {} - {}) on {} @ {}", new Object[]{keysAcked, remoteKeys.size(), acksReceived, transactionId, stateWhenInvoked});
        }

        acksReceived += keysAcked;
        assert acksReceived <= remoteKeys.size() : acksReceived + " ? " + remoteKeys.size() + " on " + transactionId;

        if (acksReceived == remoteKeys.size() && stateWhenInvoked == CoordinatorState.WAIT_FOR_ACKS) {
            state = null;
            cleanup();
        }
    }

    private void cleanup()
    {
        if (sanityCheckFireOnces) {
            synchronized(cleanedUp) {
                assert cleanedUp == false : "May only cleanUp for " + transactionId + " once";
                cleanedUp = true;
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("cleanup() on {}", new Object[]{transactionId});
        }

        BatchMutateTransactionUtil.unregisterCoordinatorKey(coordinatorKey, transactionId, localCommitTime);
        idToCoordinator.remove(transactionId);
    }

    public long waitForLocalCommitNoInterruption()
    {
        assert completionBlockingQueue != null : "can only wait on local commit";

        while (true) {
            try {
                Long completionTime = completionBlockingQueue.poll(waitForLocalCommitTimeoutSecs, TimeUnit.SECONDS);

                if (completionTime != null) {
                    return completionTime;
                } else {
                    logger.warn("BatchMutateTransaction timedout, transactionId = {}, state = {}, yes = {}, acks = {}, notifies = {}, remoteKeys.size() = {}, timestamp = {}, localCommitTime = {}, " +
                            "rT = {}, cQN = {}, sP = {}, cQYV = {}, committed = {}, cleanedUp = {}",
                            new Object[]{transactionId, state, yesVotesReceived, acksReceived, notifiesReceived, remoteKeys.size(), timestamp, localCommitTime,
                            receivedTransaction, checkedQueuedNotifications, sentPrepares, checkedQueuedYesVotes, committed, cleanedUp});
                    throw new RuntimeException();
                }
            } catch (InterruptedException e) {
                //ignore, we want a response no matter what
                if (logger.isDebugEnabled()) {
                    e.printStackTrace();
                }
                continue;
            }
        }
    }

    public Long localCommitTime()
    {
        return localCommitTime;
    }

    private static class BatchMutateTransactionCoordinatorSerializer implements IVersionedSerializer<BatchMutateTransactionCoordinator>
    {
        final static int ROW_MUTATION_FLAG = 0x01;
        final static int COUNTER_MUTATION_FLAG = 0x02;

        @Override
        public void serialize(BatchMutateTransactionCoordinator coordinator, DataOutput dos, int version) throws IOException
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

            dos.writeInt(coordinator.deps.size());
            for (Dependency dep : coordinator.deps) {
                Dependency.serializer().serialize(dep, dos);
            }

            ByteBufferUtil.writeWithShortLength(coordinator.coordinatorKey, dos);

            dos.writeInt(coordinator.allKeys.size());
            for (ByteBuffer key : coordinator.allKeys) {
                ByteBufferUtil.writeWithShortLength(key, dos);
            }

            dos.writeLong(coordinator.transactionId);

            dos.writeLong(coordinator.timestamp);
        }

        @Override
        public BatchMutateTransactionCoordinator deserialize(DataInput dis, int version) throws IOException
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

            int depsLength = dis.readInt();
            Set<Dependency> deps = new HashSet<Dependency>(depsLength);
            for (int i = 0; i < depsLength; ++i) {
                deps.add(Dependency.serializer().deserialize(dis));
            }

            ByteBuffer coordinatorKey = ByteBufferUtil.readWithShortLength(dis);

            int allKeysLength = dis.readInt();
            Set<ByteBuffer> allKeys = new HashSet<ByteBuffer>(allKeysLength);
            for (int i = 0; i < allKeysLength; ++i) {
                allKeys.add(ByteBufferUtil.readWithShortLength(dis));
            }

            long transactionId = dis.readLong();

            long timestamp = dis.readLong();

            BatchMutateTransactionCoordinator coordinator = new BatchMutateTransactionCoordinator();
            coordinator.receiveReplicatedTransaction(keyspace, mutations, deps, coordinatorKey, allKeys, transactionId, timestamp);
            return coordinator;
        }

        @Override
        public long serializedSize(BatchMutateTransactionCoordinator coordinator, int version)
        {
            int size = 0;
            size += DBConstants.shortSize + FBUtilities.encodedUTF8Length(coordinator.keyspace);

            size += DBConstants.intSize;
            for (IMutation mutation : coordinator.mutations) {
                if (mutation instanceof RowMutation) {
                    size += 1;
                    size += RowMutation.serializer().serializedSize((RowMutation) mutation, version);
                } else {
                    assert mutation instanceof CounterMutation;
                    size += 1;
                    size += CounterMutation.serializer().serializedSize((CounterMutation) mutation, version);
                }
            }

            size += DBConstants.intSize;
            for (Dependency dep : coordinator.deps) {
                size += Dependency.serializer().serializedSize(dep);
            }

            size += DBConstants.shortSize + coordinator.coordinatorKey.remaining();

            size += DBConstants.intSize;
            for (ByteBuffer key : coordinator.allKeys) {
                size += DBConstants.shortSize + key.remaining();
            }

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

    public static BatchMutateTransactionCoordinator fromBytes(byte[] raw, int version) throws IOException
    {
        return serializer().deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }

    @Override
    public Message getMessage(Integer version) throws IOException
    {
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.TRANSACTION_COORDINATOR, getSerializedBuffer(version), version);
    }
}
