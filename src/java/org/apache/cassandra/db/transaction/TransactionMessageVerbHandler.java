package org.apache.cassandra.db.transaction;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionMessageVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(TransactionMessageVerbHandler.class);

    @Override
    public void doVerb(Message message, String id)
    {
        AbstractTransactionMessage transactionMessage = null;
        try {
            transactionMessage = AbstractTransactionMessage.fromBytes(message.getMessageBody(), message.getVersion());
        } catch (IOException e) {
            logger_.error("Error in decoding transactionMessage");
        }

        if (transactionMessage instanceof NotifyMessage) {
            //call deliverNotification function in case the coordinator hasn't been created yet
            NotifyMessage notify = (NotifyMessage) transactionMessage;
            BatchMutateTransactionCoordinator.deliverNotification(notify.getTransactionId(), notify.getLocalKeyCount());

        } else if (transactionMessage instanceof PrepareMessage) {
            PrepareMessage prepare = (PrepareMessage) transactionMessage;
            BatchMutateTransactionCohort cohort = BatchMutateTransactionCohort.findCohort(prepare.getTransactionId());
            assert cohort != null;
            cohort.receivePrepare();

        } else if (transactionMessage instanceof YesVoteMessage) {
            YesVoteMessage yesVote = (YesVoteMessage) transactionMessage;
            //call deliverYesVote in case the coordinator hasn't been created yet
            BatchMutateTransactionCoordinator.deliverYesVotes(yesVote.getTransactionId(), yesVote.getLocalKeyCount());

        } else if (transactionMessage instanceof CommitMessage) {
            CommitMessage commit = (CommitMessage) transactionMessage;
            BatchMutateTransactionCohort cohort = BatchMutateTransactionCohort.findCohort(commit.getTransactionId());
            assert cohort != null;
            cohort.receiveCommit(commit.getCommitTime());

        } else if (transactionMessage instanceof AckMessage) {
            AckMessage ack = (AckMessage) transactionMessage;
            BatchMutateTransactionCoordinator coordinator = BatchMutateTransactionCoordinator.findCoordinator(ack.getTransactionId());
            assert coordinator != null;
            coordinator.receiveAck(ack.getLocalKeyCount());

        } else if (transactionMessage instanceof CheckTransactionMessage) {
            CheckTransactionMessage check = (CheckTransactionMessage) transactionMessage;
            TransactionsCheckedMessage checkedReply = new TransactionsCheckedMessage(BatchMutateTransactionUtil.checkTransactions(check));

            Message response;
            try {
                response = checkedReply.getReply(message);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (logger_.isDebugEnabled())
                logger_.debug("Sending " + checkedReply + " to " + id + "@" + message.getFrom());
            MessagingService.instance().sendReply(response, id, message.getFrom());
        } else {
            assert false : "Unknown message type: " + transactionMessage.getClass();
        }
    }
}
