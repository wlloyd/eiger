package org.apache.cassandra.db.transaction;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.CachingMessageProducer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ShortNodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class TransactionProxy
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionProxy.class);
    public static final long FAKE_PENDING_TRANSACTION_ID = Long.MIN_VALUE;

    private TransactionProxy() {}

    private static void sendTransactionMessage(String keyspace, ByteBuffer key, AbstractTransactionMessage transactionMessage)
    throws IOException
    {
        List<InetAddress> localEndpoints = StorageService.instance.getLocalLiveNaturalEndpoints(keyspace, key);
        assert localEndpoints.size() == 1 : "Assumed for now";
        InetAddress localEndpoint = localEndpoints.get(0);

        MessagingService.instance().sendOneWay(transactionMessage.getMessage(Gossiper.instance.getVersion(localEndpoint)), localEndpoint);
    }

    private static void sendTransactionMessages(String keyspace, List<ByteBuffer> keys, AbstractTransactionMessage transactionMessage)
    throws IOException
    {
        Set<InetAddress> allLocalEndpoints = new HashSet<InetAddress>();

        for (ByteBuffer key : keys) {
            List<InetAddress> localEndpoints = StorageService.instance.getLocalLiveNaturalEndpoints(keyspace, key);
            assert localEndpoints.size() == 1 : "Assumed for now";
            InetAddress localEndpoint = localEndpoints.get(0);

            allLocalEndpoints.add(localEndpoint);
        }

        for (InetAddress endpoint : allLocalEndpoints) {
            MessagingService.instance().sendOneWay(transactionMessage.getMessage(Gossiper.instance.getVersion(endpoint)), endpoint);
        }
    }

    public static void sendNotify(String keyspace, ByteBuffer coordinatorKey, long transactionId, int localKeyCount)
    throws IOException
    {
        sendTransactionMessage(keyspace, coordinatorKey, new NotifyMessage(transactionId, localKeyCount));
    }

    public static void sendYesVote(String keyspace, ByteBuffer coordinatorKey, long transactionId, int localKeyCount)
    throws IOException
    {
        sendTransactionMessage(keyspace, coordinatorKey, new YesVoteMessage(transactionId, localKeyCount));
    }

    public static void sendAck(String keyspace, ByteBuffer coordinatorKey, long transactionId, int localKeyCount)
    throws IOException
    {
        sendTransactionMessage(keyspace, coordinatorKey, new AckMessage(transactionId, localKeyCount));
    }


    public static void sendPrepares(String keyspace, List<ByteBuffer> remoteKeys, long transactionId)
    throws IOException
    {
        sendTransactionMessages(keyspace, remoteKeys, new PrepareMessage(transactionId));
    }

    public static void sendCommit(String keyspace, List<ByteBuffer> remoteKeys, long transactionId, long commitTime)
    throws IOException
    {
        sendTransactionMessages(keyspace, remoteKeys, new CommitMessage(transactionId, commitTime));
    }

    // only for use in testing and microbenchmarking
    public static void forceCheckTransaction(String keyspace, long checkTime) throws IOException
    {
        Set<InetAddress> nonLocalAddresses = ShortNodeId.getNonLocalAddressesInThisDC();
        assert nonLocalAddresses.size() > 0 : "Forced indirection requires > 1 nodes/dc";
        InetAddress coordinator = nonLocalAddresses.iterator().next();
        List<Long> transactionIds = Collections.singletonList(FAKE_PENDING_TRANSACTION_ID);
        CheckTransactionMessage check = new CheckTransactionMessage(transactionIds, checkTime);
        CheckTransactionCallback callback = new CheckTransactionCallback(checkTime);

        MessagingService.instance().sendRR(check, coordinator, callback);
    }


    public static void checkTransactions(String keyspace, Set<Long> pendingTransactionIds, long checkTime) throws IOException
    {
        //combine all transactionIds that are being sent to the same endpoint
        Map<InetAddress, List<Long>> endpointToTransactionIds = new HashMap<InetAddress, List<Long>>();
        for (Long transactionId : pendingTransactionIds) {
            ByteBuffer coordinatorKey = BatchMutateTransactionUtil.findCoordinatorKey(transactionId);

            List<InetAddress> endpoints = StorageService.instance.getLocalLiveNaturalEndpoints(keyspace, coordinatorKey);
            assert endpoints.size() == 1 : "Assumed for now";
            InetAddress coordinator = endpoints.get(0);

            if (!endpointToTransactionIds.containsKey(coordinator)) {
                endpointToTransactionIds.put(coordinator, new ArrayList<Long>());
            }
            endpointToTransactionIds.get(coordinator).add(transactionId);
        }

        List<CheckTransactionCallback> callbacks = new ArrayList<CheckTransactionCallback>(endpointToTransactionIds.size());
        for (Entry<InetAddress, List<Long>> entry : endpointToTransactionIds.entrySet()) {
            InetAddress coordinator = entry.getKey();
            List<Long> transactionIds = entry.getValue();
            CheckTransactionMessage check = new CheckTransactionMessage(transactionIds, checkTime);
            CheckTransactionCallback callback = new CheckTransactionCallback(checkTime);

            //WL TODO: These should be sent in parallel!
            MessagingService.instance().sendRR(check, coordinator, callback);
        }
    }

    //WL TODO: Remove the assumption that datacenters have matching keyranges
    private static void replicateTransactionToOtherDatacenters(MessageProducer producer, String keyspace, ByteBuffer targetKey)
    throws IOException
    {
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, targetKey);
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        IWriteResponseHandler ignoredResponseHandler = WriteResponseHandler.create(endpoints, ConsistencyLevel.ALL, keyspace);

        // code follows the structure of StorageProxy.sendToHintedEndpoints
        // Multimap that holds onto all the messages and addresses meant for a specific datacenter
        Map<String, Multimap<Message, InetAddress>> dcMessages = new HashMap<String, Multimap<Message, InetAddress>>(endpoints.size());

        for (InetAddress destination : endpoints) {
            assert FailureDetector.instance.isAlive(destination) : "Not dealing with failed nodes in the dc right now";

            if (destination.equals(FBUtilities.getBroadcastAddress())) {
                //no need to *replicate* to the local machine, it should already have started handling the transaction
                continue;
            }

            if (logger.isDebugEnabled())
                logger.debug("replicate transaction to " + destination);

            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);
            Multimap<Message, InetAddress> messages = dcMessages.get(dc);
            if (messages == null) {
                messages = HashMultimap.create();
                dcMessages.put(dc, messages);
            }

            messages.put(producer.getMessage(Gossiper.instance.getVersion(destination)), destination);
        }

        StorageProxy.sendMessages(localDataCenter, dcMessages, ignoredResponseHandler);
    }

    public static void replicateCoordinatorToOtherDatacenters(BatchMutateTransactionCoordinator coordinator, String keyspace, ByteBuffer coordinatorKey)
    throws IOException
    {
        MessageProducer producer = new CachingMessageProducer(coordinator);
        replicateTransactionToOtherDatacenters(producer, keyspace, coordinatorKey);
    }

    public static void replicateCohortToOtherDatacenters(BatchMutateTransactionCohort cohort, String keyspace, ByteBuffer cohortKey)
    throws IOException
    {
        MessageProducer producer = new CachingMessageProducer(cohort);
        replicateTransactionToOtherDatacenters(producer, keyspace, cohortKey);
    }
}
