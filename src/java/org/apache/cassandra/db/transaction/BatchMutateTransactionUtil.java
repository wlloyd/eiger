package org.apache.cassandra.db.transaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.LamportClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMutateTransactionUtil
{
    private static final Logger logger = LoggerFactory.getLogger(BatchMutateTransactionUtil.class);
    private static Map<Long, ByteBuffer> transactionIdToCoordinatorKey = new ConcurrentHashMap<Long, ByteBuffer>();
    //WL TODO: Garbage Collect Old Entries in transactionIdToCommitTime
    private static Map<Long, Long> transactionIdToLocalCommitTime = new ConcurrentHashMap<Long, Long>();

    public static class CommitOrNotYetTime
    {
        public final Long commitTime;
        public final Long notYetCommittedTime;

        public CommitOrNotYetTime(Long commitTime, Long notYetCommittedTime)
        {
            assert commitTime == null || notYetCommittedTime == null;
            this.commitTime = commitTime;
            this.notYetCommittedTime = notYetCommittedTime;
        }
    }
    private static Map<Long, CommitOrNotYetTime> checkedTransactions = new HashMap<Long, CommitOrNotYetTime>();

    private BatchMutateTransactionUtil() {
        //don't instantiate, helper functions only
    }

    public static void registerCoordinatorKey(ByteBuffer coordinatorKey, long transactionId)
    {
        ByteBuffer previousValue = transactionIdToCoordinatorKey.put(transactionId, coordinatorKey);
        assert previousValue == null : "transactionIds should be unique";
    }

    public static void unregisterCoordinatorKey(ByteBuffer coordinatorKey, long transactionId, long localCommitTime)
    {
        transactionIdToLocalCommitTime.put(transactionId, localCommitTime);
        ByteBuffer removedKey = transactionIdToCoordinatorKey.remove(transactionId);
        assert removedKey == coordinatorKey;
    }

    public static ByteBuffer findCoordinatorKey(Long transactionId)
    {
        return transactionIdToCoordinatorKey.get(transactionId);
    }

    public static List<IMutation> convertToInternalMutations(String keyspace, Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ByteBuffer coordinatorKey)
    throws InvalidRequestException
    {
        //the timestamp and localCommitTime are set when we apply the transaction, so we'll set them to invalid values here
        long timestamp = Long.MIN_VALUE;
        long localCommitTime = Long.MIN_VALUE;

        List<IMutation> rowMutations = new ArrayList<IMutation>();

        //convert from thrift mutation_map to internal list of IMutations
        //code is copied from CassandraServer.batch_mutate_internal - permission checking
        //Note, permission was checked when the thrift interface received the transaction.
        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet()) {
            ByteBuffer key = mutationEntry.getKey();

            // We need to separate row mutation for standard cf and counter cf (that will be encapsulated in a
            // CounterMutation) because it doesn't follow the same code path
            RowMutation rmStandard = null;
            RowMutation rmCounter = null;

            Map<String, List<Mutation>> columnFamilyToMutations = mutationEntry.getValue();
            for (Map.Entry<String, List<Mutation>> columnFamilyMutations : columnFamilyToMutations.entrySet()) {
                String cfName = columnFamilyMutations.getKey();

                CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
                ThriftValidation.validateKey(metadata, key);

                RowMutation rm;
                if (metadata.getDefaultValidator().isCommutative()) {
                    ThriftValidation.validateCommutativeForWrite(metadata, ConsistencyLevel.ONE);
                    rmCounter = rmCounter == null ? new RowMutation(keyspace, key) : rmCounter;
                    rm = rmCounter;
                } else {
                    rmStandard = rmStandard == null ? new RowMutation(keyspace, key) : rmStandard;
                    rm = rmStandard;
                }

                for (Mutation mutation : columnFamilyMutations.getValue())
                {
                    ThriftValidation.validateMutation(metadata, mutation);

                    if (mutation.deletion != null)
                    {
                        rm.deleteColumnOrSuperColumn(cfName, mutation.deletion, timestamp, localCommitTime, coordinatorKey);
                    }
                    if (mutation.column_or_supercolumn != null)
                    {
                        rm.addColumnOrSuperColumn(cfName, mutation.column_or_supercolumn, timestamp, localCommitTime, coordinatorKey);
                    }
                }
            }
            if (rmStandard != null && !rmStandard.isEmpty())
                rowMutations.add(rmStandard);
            if (rmCounter != null && !rmCounter.isEmpty())
                rowMutations.add(new org.apache.cassandra.db.CounterMutation(rmCounter, ConsistencyLevel.ONE));
        }

        logger.debug("Mutations are {}", rowMutations);
        return rowMutations;
    }

    public static void markTransactionPending(String keyspace, List<IMutation> mutations, long transactionId)
    throws InvalidRequestException, IOException
    {
        long pendingTime = LamportClock.getVersion();

        for (IMutation mutation : mutations) {
            mutation.convertToPending(pendingTime, transactionId).apply();
        }
    }

    public static void applyTransaction(String keyspace, List<IMutation> mutations, long timestamp, long localCommitTime, ByteBuffer coordinatorKey)
    throws InvalidRequestException, IOException
    {
        for (IMutation mutation : mutations) {
            mutation.updateTimestamps(timestamp, localCommitTime);
        }

        //apply mutations locally, note these are not applied atomically, but that's fine because get_transactions will ensure they are seen atomically
        for (final IMutation mutation : mutations) {
            if (mutation instanceof CounterMutation) {
                assert false : "Not yet handled";
                //responseHandlers.add(mutateCounter((CounterMutation)mutation, localDataCenter));
            } else {
                assert mutation instanceof RowMutation;
                mutation.apply();
            }
        }
    }

    /**
     * @param check
     * @return map from transactionId to commitTime, or Long.MIN_VALUE if it's not yet committed
     */
    public static Map<Long, Long> checkTransactions(CheckTransactionMessage check)
    {
        // Special Case: A forced checkTransaction, just return true in this case
        if (check.transactionIds.size() == 1 && check.transactionIds.iterator().next().equals(TransactionProxy.FAKE_PENDING_TRANSACTION_ID)) {
            logger.trace("Responding to checkTransaction for fake tranasaction, time={}", check.checkTime);
            Map<Long, Long> transactionIdToChecked = new HashMap<Long, Long>(check.transactionIds.size());
            transactionIdToChecked.put(TransactionProxy.FAKE_PENDING_TRANSACTION_ID, 0L);
            return transactionIdToChecked;
        }

        //WL TODO: Could reduce the number of checkTransaction needed by returning the commitTime no matter what, if we know what it is

        long checkTime = check.checkTime;

        Map<Long, Long> transactionIdToChecked = new HashMap<Long, Long>(check.transactionIds.size());

        for (Long transactionId : check.transactionIds) {
            BatchMutateTransactionCoordinator coordinator = BatchMutateTransactionCoordinator.findCoordinator(transactionId);
            Long commitTime;
            if (coordinator != null) {
                commitTime = coordinator.localCommitTime();
            } else {
                commitTime = transactionIdToLocalCommitTime.get(transactionId);
            }

            //if commitTime after checkTime, set the value to null to signal this
            if (commitTime == null || commitTime > checkTime) {
                commitTime = Long.MIN_VALUE;
            }

            transactionIdToChecked.put(transactionId, commitTime);
        }

        return transactionIdToChecked;
    }

    public static void updateCheckedTransaction(Map<Long, Long> transactionIdToResult, long checkedTime)
    {
        for (Entry<Long, Long> entry : transactionIdToResult.entrySet()) {
            long transactionId = entry.getKey();
            Long commitTime = entry.getValue();

            if (commitTime != Long.MIN_VALUE) {
                checkedTransactions.put(transactionId, new CommitOrNotYetTime(commitTime, null));
            } else {
                //not yet committed as of checkedTime
                if (checkedTransactions.containsKey(transactionId)) {
                    CommitOrNotYetTime conyt = checkedTransactions.get(transactionId);
                    if (conyt.commitTime != null) {
                        //already committed, don't update
                        assert commitTime > checkedTime;
                        continue;
                    } else {
                        //ensure notYetCommittedTime is monotonically increasing
                        long notYetCommittedTime = Math.max(conyt.notYetCommittedTime, checkedTime);
                        checkedTransactions.put(transactionId, new CommitOrNotYetTime(null, notYetCommittedTime));
                    }
                } else {
                    checkedTransactions.put(transactionId, new CommitOrNotYetTime(null, checkedTime));
                }
            }
        }
    }

    public static CommitOrNotYetTime findCheckedTransactionResult(Long transactionId)
    {
        return checkedTransactions.get(transactionId);
    }
}
