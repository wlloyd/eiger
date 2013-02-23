package org.apache.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.transaction.BatchMutateTransactionUtil;
import org.apache.cassandra.db.transaction.BatchMutateTransactionUtil.CommitOrNotYetTime;
import org.apache.cassandra.db.transaction.PendingTransactionColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.LamportClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftConverter
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftConverter.class);

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();

    private ThriftConverter() {
        //do not instantiate, helper functions only
    }

    private static CounterColumn thriftifyCounterColumn(org.apache.cassandra.db.CounterColumn column)
    {
        assert !column.isMarkedForDelete() : "Deleted columns are always normal Columns, not CounterColumns";
        CounterColumn thrift_column = new CounterColumn(column.name(), CounterContext.instance().total(column.value()));
        thrift_column.setTimestampToCoordinatorKey(CounterContext.instance().timestampToCoordinatorKey(column.value()));
        thrift_column.setEarliest_valid_time(column.earliestValidTime());
        thrift_column.setLatest_valid_time(column.isSetLatestValidTime() ? column.latestValidTime() : LamportClock.getVersion());
        //WL TODO: Add the vector of transactionCoordinatorKeys here
        return thrift_column;
    }

    private static Column thriftifyDeletedColumn(org.apache.cassandra.db.DeletedColumn column)
    {
        Column deleted_column = new Column(column.name()).setDeleted_time(column.getMarkedForDeleteAt());
        deleted_column.setEarliest_valid_time(column.earliestValidTime());
        deleted_column.setLatest_valid_time(column.isSetLatestValidTime() ? column.latestValidTime() : LamportClock.getVersion());
        ByteBuffer transactionCoordinatorKey = column.transactionCoordinatorKey();
        if (transactionCoordinatorKey != null) {
            deleted_column.setTransactionCoordinatorKey(transactionCoordinatorKey);
        }
        return deleted_column;
    }

    private static Column thriftifyPendingTransactionColumn(PendingTransactionColumn column)
    {
        Column ptc_column = new Column(column.name());
        return ptc_column;
    }

    private static Column thriftifyColumn(org.apache.cassandra.db.Column column)
    {
        if (column instanceof PendingTransactionColumn) {
            return thriftifyPendingTransactionColumn((PendingTransactionColumn) column);
        } else if (column instanceof org.apache.cassandra.db.DeletedColumn) {
            return thriftifyDeletedColumn((DeletedColumn) column);
        }

        Column thrift_column = new Column(column.name()).setValue(column.value()).setTimestamp(column.timestamp());
        thrift_column.setEarliest_valid_time(column.earliestValidTime());
        thrift_column.setLatest_valid_time(column.isSetLatestValidTime() ? column.latestValidTime() : LamportClock.getVersion());
        if (column instanceof ExpiringColumn) {
            thrift_column.setTtl(((ExpiringColumn) column).getTimeToLive());
        }
        ByteBuffer transactionCoordinatorKey = column.transactionCoordinatorKey();
        if (transactionCoordinatorKey != null) {
            thrift_column.setTransactionCoordinatorKey(transactionCoordinatorKey);
        }
        return thrift_column;
    }

    private static ColumnOrSuperColumn markFirstRoundResultAsValid(org.apache.cassandra.db.Column currentColumn)
    {
        Column thrift_column = new Column(currentColumn.name());
        thrift_column.setFirst_round_was_valid(true);
        thrift_column.setLatest_valid_time(currentColumn.earliestValidTime()-1);
        return wrapInCOSC(thrift_column);
    }

    private static ColumnOrSuperColumn wrapInCOSC(Object column)
    {
        if (column == null) {
            return null;
        } else if (column instanceof Column) {
            return new ColumnOrSuperColumn().setColumn((Column) column);
        } else if (column instanceof CounterColumn) {
            return new ColumnOrSuperColumn().setCounter_column((CounterColumn) column);
        } else if (column instanceof SuperColumn) {
            return new ColumnOrSuperColumn().setSuper_column((SuperColumn) column);
        } else if (column instanceof CounterSuperColumn) {
            return new ColumnOrSuperColumn().setCounter_super_column((CounterSuperColumn) column);
        } else {
            assert false : "Can only wrap thrift column types";
            return null;
        }
    }

    private static ColumnOrSuperColumn thriftifyIColumn(IColumn column)
    {
        if (column instanceof org.apache.cassandra.db.CounterColumn) {
            return wrapInCOSC(thriftifyCounterColumn((org.apache.cassandra.db.CounterColumn) column));
        } else {
            return wrapInCOSC(thriftifyColumn((org.apache.cassandra.db.Column) column));
        }
    }

    private static SuperColumn thriftifySuperColumn(org.apache.cassandra.db.SuperColumn superColumn)
    {
        if (superColumn.getSubColumns() == null || superColumn.getSubColumns().isEmpty()) {
            if (superColumn.isMarkedForDelete()) {
                return new SuperColumn(superColumn.name(), new ArrayList<Column>()).setDeleted_time(superColumn.getMarkedForDeleteAt());
            }
            return null;
        }

        List<Column> subcolumns = new ArrayList<Column>(superColumn.getSubColumns().size());
        for (IColumn subcolumn : superColumn.getSubColumns()) {
            subcolumns.add(thriftifyColumn((org.apache.cassandra.db.Column) subcolumn));
        }
        return new SuperColumn(superColumn.name(), subcolumns);
    }

    private static CounterSuperColumn thriftifyCounterSuperColumn(org.apache.cassandra.db.SuperColumn counterSuperColumn)
    {
        if (counterSuperColumn.getSubColumns() == null || counterSuperColumn.getSubColumns().isEmpty()) {
            if (counterSuperColumn.isMarkedForDelete()) {
                return new CounterSuperColumn(counterSuperColumn.name(), new ArrayList<CounterColumn>()).setDeleted_time(counterSuperColumn.getMarkedForDeleteAt());
            }
            return null;
        }

        ArrayList<CounterColumn> subcolumns = new ArrayList<CounterColumn>(counterSuperColumn.getSubColumns().size());
        for (IColumn column : counterSuperColumn.getSubColumns()) {
            subcolumns.add(thriftifyCounterColumn((org.apache.cassandra.db.CounterColumn) column));
        }
        return new CounterSuperColumn(counterSuperColumn.name(), subcolumns);
    }

    public static List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<IColumn> columns, boolean reverseOrder, boolean isCounterCF)
    {
        List<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column : columns) {
            ColumnOrSuperColumn superColumn;
            if (isCounterCF) {
                superColumn = wrapInCOSC(thriftifyCounterSuperColumn((org.apache.cassandra.db.SuperColumn) column));
            } else {
                superColumn = wrapInCOSC(thriftifySuperColumn((org.apache.cassandra.db.SuperColumn) column));
            }
            if (superColumn != null) {
                thriftSuperColumns.add(superColumn);
            }
        }

        if (reverseOrder) {
            Collections.reverse(thriftSuperColumns);
        }
        return thriftSuperColumns;
    }

    public static List<ColumnOrSuperColumn> thriftifyColumnFamily(ColumnFamily cf, boolean subcolumnsOnly, boolean reverseOrder)
    {
        if (cf == null || cf.isEmpty()) {
            if (cf != null && cf.isMarkedForDelete()) {
                return Collections.singletonList(wrapInCOSC(new Column(ByteBufferUtil.bytes("ColumnFamily")).setDeleted_time(cf.getMarkedForDeleteAt())));
            }
            return EMPTY_COLUMNS;
        } else if (subcolumnsOnly){
            IColumn column = cf.iterator().next();
            Collection<IColumn> subcolumns = column.getSubColumns();
            if (subcolumns == null || subcolumns.isEmpty()) {
                if (column.isMarkedForDelete()) {
                    return Collections.singletonList(wrapInCOSC(new Column(column.name()).setDeleted_time(column.getMarkedForDeleteAt())));
                } else {
                    return EMPTY_COLUMNS;
                }
            }
            else {
                return thriftifyIColumns(subcolumns, reverseOrder);
            }
        } else if (cf.isSuper()){
            boolean isCounterCF = cf.metadata().getDefaultValidator().isCommutative();
            return thriftifySuperColumns(cf.getSortedColumns(), reverseOrder, isCounterCF);
        } else {
            return thriftifyIColumns(cf.getSortedColumns(), reverseOrder);
        }
    }

    public static List<ColumnOrSuperColumn> thriftifyColumnFamilyAtTime(ColumnFamily cf, boolean subcolumnsOnly, boolean reverseOrder, long chosenTime)
    {
        assert false : "Still in progress";
        return null;
//
//        if (cf == null || cf.isEmpty()) {
//            assert false : "Didn't reason about this"; // TODO add old version support to column families too
//            if (cf != null && cf.isMarkedForDelete()) {
//                return Collections.singletonList(wrapInCOSC(new Column(ByteBufferUtil.bytes("ColumnFamily")).setDeleted_time(cf.getMarkedForDeleteAt())));
//            }
//            return EMPTY_COLUMNS;
//        } else if (subcolumnsOnly) {
//            IColumn column = cf.iterator().next();
//            ColumnOrSuperColumn superColumn = selectChosenColumn(column, chosenTime);
//
//            List<ColumnOrSuperColumn> subcolumns = new ArrayList<ColumnOrSuperColumn>(superColumn.super_column.columns.size());
//            for (Column subcolumn : superColumn.super_column.columns) {
//                subcolumns.add(wrapInCOSC(subcolumn));
//            }
//
//            if (reverseOrder) {
//                Collections.reverse(subcolumns);
//            }
//            return subcolumns;
//        } else if (cf.isSuper()) {
//            boolean isCounterCF = cf.metadata().getDefaultValidator().isCommutative();
//            List<ColumnOrSuperColumn> superColumns = new ArrayList<ColumnOrSuperColumn>();
//            for (IColumn superColumn : cf.getSortedColumns()) {
//                superColumns.add(selectChosenColumn(superColumn, chosenTime));
//            }
//
//            if (reverseOrder) {
//                Collections.reverse(superColumns);
//            }
//            return superColumns;
//        } else {
//            List<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();
//            for (IColumn column : cf.getSortedColumns()) {
//                columns.add(selectChosenColumn(column, chosenTime));
//            }
//
//            if (reverseOrder) {
//                Collections.reverse(columns);
//            }
//            return columns;
//        }
    }

    private static List<ColumnOrSuperColumn> thriftifyIColumns(Collection<IColumn> subcolumns, boolean reverseOrder)
    {

        ArrayList<ColumnOrSuperColumn> thriftColumns = new ArrayList<ColumnOrSuperColumn>(subcolumns.size());
        for (IColumn column : subcolumns) {
            thriftColumns.add(thriftifyIColumn(column));
        }

        // we have to do the reversing here, since internally we pass results around in ColumnFamily
        // objects, which always sort their columns in the "natural" order
        // TODO this is inconvenient for direct users of StorageProxy
        if (reverseOrder)
            Collections.reverse(thriftColumns);

        return thriftColumns;
    }

    public static List<KeySlice> thriftifyKeySlicesAtTime(List<Row> rows, ColumnParent column_parent, SlicePredicate predicate, long chosenTime)
    {
        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamilyAtTime(row.cf, column_parent.super_column != null, reversed, chosenTime);
            keySlices.add(new KeySlice(row.key.key, thriftifiedColumns));
        }

        return keySlices;
    }


    public static List<KeySlice> thriftifyKeySlices(List<Row> rows, ColumnParent column_parent, SlicePredicate predicate)
    {
        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = ThriftConverter.thriftifyColumnFamily(row.cf, column_parent.super_column != null, reversed);
            keySlices.add(new KeySlice(row.key.key, thriftifiedColumns));
        }

        return keySlices;
    }


    /** Find the transactionIds of all pending transaction that could affect this column at the chosenTime
     * Also update any checked transactions ids that can affect this transaction
     * @param chosenColumn
     * @param chosenTime
     * @param currentlyVisibleColumn This is not necessary the chosen column, it contains the list of previousVersions
     * @return Relevant pending transactionIds, or null if none
     */
    private static Set<Long> findAndUpdatePendingTransactions(org.apache.cassandra.db.Column chosenColumn, long chosenTime, org.apache.cassandra.db.Column currentlyVisibleColumn)
    {
        if (!(chosenColumn instanceof PendingTransactionColumn) &&
                (!chosenColumn.isSetLatestValidTime() || chosenTime <= chosenColumn.latestValidTime())) {
            //if the chosenColumn isn't a PTC and EVT < chosen < LVT then no pending transactions
            assert chosenColumn.earliestValidTime() <= chosenTime;
            return null;
        } else {
            Set<Long> pendingTransactionIds = new HashSet<Long>();
            //lock for use of previousVersions
            synchronized (currentlyVisibleColumn) {
                for (IColumn oldColumn : currentlyVisibleColumn.previousVersions()) {
                    if (oldColumn.earliestValidTime() > chosenTime) {
                        continue;
                    } else {
                        if (oldColumn instanceof PendingTransactionColumn) {
                            long transactionId = ((PendingTransactionColumn) oldColumn).getTransactionId();
                            CommitOrNotYetTime checkResult = BatchMutateTransactionUtil.findCheckedTransactionResult(transactionId);
                            if (checkResult == null) {
                                pendingTransactionIds.add(transactionId);
                            } else {
                                applyCheckTransactionUpdate(currentlyVisibleColumn, transactionId, checkResult);
                            }
                        }
                    }
                }
            }
            return pendingTransactionIds.size() == 0 ? null : pendingTransactionIds;
        }
    }

    //Assumes the lock on currentlyVisibleColumn is already held, so we can update it's previousVersions
    private static void applyCheckTransactionUpdate(org.apache.cassandra.db.Column currentlyVisibleColumn, long transactionId, CommitOrNotYetTime checkResult)
    {
        long newEarliestValidTime = checkResult.commitTime != null ? checkResult.commitTime : checkResult.notYetCommittedTime;

        //To simplify this code I'll temporarily add the current versions to previousVersions to get all versions
        //but it must be removed before the function returns
        NavigableSet<IColumn> allVersions = currentlyVisibleColumn.previousVersions();
        allVersions.add(currentlyVisibleColumn);

        //first pass, find the update PTC and determine the minimumPendingTransactionTIme
        PendingTransactionColumn updatedColumn = null;
        Long minPendingTransactionTime = Long.MAX_VALUE;
        for (IColumn column : allVersions.descendingSet()) {
            if (column instanceof PendingTransactionColumn) {
                if (((PendingTransactionColumn) column).getTransactionId() == transactionId) {
                    updatedColumn = (PendingTransactionColumn) column;
                    if (checkResult.commitTime == null) {
                        minPendingTransactionTime = Math.min(checkResult.notYetCommittedTime, minPendingTransactionTime);
                    }
                } else {
                    minPendingTransactionTime = Math.min(column.earliestValidTime(), minPendingTransactionTime);
                }
            }
        }
        //TODO just return if updatedColumn is null
        assert updatedColumn != null : "This is actually fine, but don't expect this initially";

        //only do the update if we're actually moving the evt forward
        if (updatedColumn != null && newEarliestValidTime > updatedColumn.earliestValidTime()) {

            //remove the updatedColumn and reinsert it with its new EVT
            boolean removed = allVersions.remove(updatedColumn);
            assert removed == true;
            updatedColumn.setEarliestValidTime(newEarliestValidTime);
            allVersions.add(updatedColumn);

            //second pass, update all LVTs
            Long previousEVT = null;
            for (IColumn column : allVersions) {
                if (minPendingTransactionTime == Long.MAX_VALUE) {
                    column.setLatestValidTime(previousEVT);
                } else if (minPendingTransactionTime <= column.earliestValidTime()) {
                    column.setLatestValidTime(column.earliestValidTime());
                } else {
                    assert minPendingTransactionTime > column.earliestValidTime();
                    column.setLatestValidTime(Math.min(minPendingTransactionTime, previousEVT));
                }
                previousEVT = column.earliestValidTime();
            }
        }

        //remove the visible version from previousVersion before returning
        allVersions.remove(currentlyVisibleColumn);
    }

    public static class ChosenColumnResult
    {
        final public ColumnOrSuperColumn cosc;
        final public boolean pendingTransaction;
        final public Set<Long> transactionIds;

        public ChosenColumnResult(ColumnOrSuperColumn cosc, Set<Long> transactionIds)
        {
            this.cosc = cosc;
            if (transactionIds == null || transactionIds.size() == 0) {
                this.pendingTransaction = false;
                this.transactionIds = null;
            } else {
                this.pendingTransaction = true;
                this.transactionIds = transactionIds;
            }
        }
    }

    /**
    *
    * @param column
    * @param chosenTime
    * @return thriftified version of the column valid at the chosenTime
    */
   public static ChosenColumnResult selectChosenColumn(IColumn column, long chosenTime)
   {
       if (column instanceof org.apache.cassandra.db.SuperColumn) {
           List<Column> chosenSubcolumns = new ArrayList<Column>();
           Set<Long> pendingTransactions = new HashSet<Long>();
           for (IColumn subcolumn : column.getSubColumns()) {
               ChosenColumnResult ccr = selectChosenColumn(subcolumn, chosenTime);
               chosenSubcolumns.add(ccr.cosc.column);
               if (ccr.transactionIds != null) {
                   pendingTransactions.addAll(ccr.transactionIds);
               }
           }
           return new ChosenColumnResult(wrapInCOSC(new SuperColumn(column.name(), chosenSubcolumns)), pendingTransactions);
       } else {
           assert column instanceof org.apache.cassandra.db.Column;
           org.apache.cassandra.db.Column currentlyVisibleColumn = (org.apache.cassandra.db.Column) column;

           assert chosenTime < LamportClock.getVersion() : "Client can't chose a logical time in the future";

           if (logger.isTraceEnabled()) {
               if (column.previousVersions() != null) {
                   String previousVersions = new String();
                   for (IColumn oldColumn : column.previousVersions()) {
                       previousVersions += ", " + oldColumn.earliestValidTime() + "-";
                   }
                   previousVersions = "{" + previousVersions.substring(2) + "}";
                   logger.trace("picking chosenTime={} from previousVersons={}, current={}-", new Object[]{chosenTime, previousVersions, currentlyVisibleColumn.earliestValidTime()});
               } else {
                   logger.trace("picking chosenTime={} from previousVersons=[], current={}-", chosenTime, currentlyVisibleColumn.earliestValidTime());
               }
           }


           if (column.earliestValidTime() <= chosenTime) {
               Set<Long> pendingTransactionIds = findAndUpdatePendingTransactions((org.apache.cassandra.db.Column) column, chosenTime, currentlyVisibleColumn);
               return new ChosenColumnResult(thriftifyIColumn(column), pendingTransactionIds);
           } else {
               if (column.previousVersions() != null) {
                   for (IColumn oldColumn : column.previousVersions()) {
                       // goes through column by most recent first
                       if (oldColumn.earliestValidTime() <= chosenTime) {
                           Set<Long> pendingTransactionIds =  findAndUpdatePendingTransactions((org.apache.cassandra.db.Column) oldColumn, chosenTime, currentlyVisibleColumn);
                           return new ChosenColumnResult(thriftifyIColumn(oldColumn), pendingTransactionIds);
                       }
                   }
               }

               // If no previous version matches, then the client's first round read value is valid
               return new ChosenColumnResult(markFirstRoundResultAsValid(currentlyVisibleColumn), new HashSet<Long>());
           }
       }
   }
}
