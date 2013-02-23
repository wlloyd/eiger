/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.thrift;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.client.ClientContext;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.migration.*;
import org.apache.cassandra.db.transaction.BatchMutateTransactionCohort;
import org.apache.cassandra.db.transaction.BatchMutateTransactionCoordinator;
import org.apache.cassandra.db.transaction.TransactionProxy;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.SocketSessionManagementService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ThriftConverter.ChosenColumnResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ColumnOrSuperColumnHelper;
import org.apache.cassandra.utils.ColumnOrSuperColumnHelper.EvtAndLvt;
import org.apache.cassandra.utils.LamportClock;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

public class CassandraServer implements Cassandra.Iface
{
    private static Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    private final static int COUNT_PAGE_SIZE = 1024;

    // thread local state containing session information
    public final ThreadLocal<ClientState> clientState = new ThreadLocal<ClientState>()
    {
        @Override
        public ClientState initialValue()
        {
            return new ClientState();
        }
    };

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
    }

    public ClientState state()
    {
        SocketAddress remoteSocket = SocketSessionManagementService.remoteSocket.get();
        ClientState retval = null;
        if (null != remoteSocket)
        {
            retval = SocketSessionManagementService.instance.get(remoteSocket);
            if (null == retval)
            {
                retval = new ClientState();
                SocketSessionManagementService.instance.put(remoteSocket, retval);
            }
        }
        else
        {
            retval = clientState.get();
        }
        return retval;
    }

    protected Map<DecoratedKey, ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<DecoratedKey, ColumnFamily> columnFamilyKeyMap = new HashMap<DecoratedKey, ColumnFamily>();

        List<Row> rows;
        try
        {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                rows = StorageProxy.read(commands, consistency_level);
            }
            finally
            {
                release();
            }
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
        	throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key, row.cf);
        }
        return columnFamilyKeyMap;
    }


    private interface ISliceMap
    {
    }

    private class ThriftifiedSliceMap implements ISliceMap
    {
        public final Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftifiedMap;

        public ThriftifiedSliceMap(Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftifiedMap)
        {
            this.thriftifiedMap = thriftifiedMap;
        }
    }

    private class InternalSliceMap implements ISliceMap
    {
        public final Map<ByteBuffer, Collection<IColumn>> cassandraMap;

        public InternalSliceMap(Map<ByteBuffer, Collection<IColumn>> cassandraMap)
        {
            this.cassandraMap = cassandraMap;
        }
    }

    private ISliceMap getSlice(List<ReadCommand> commands, ConsistencyLevel consistency_level, boolean thriftify)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<DecoratedKey, ColumnFamily> columnFamilies = readColumnFamily(commands, consistency_level);
        if (thriftify) {
            Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftifiedColumnFamiliesMap = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
            for (ReadCommand command: commands)
            {
                ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(command.key));
                boolean reverseOrder = command instanceof SliceFromReadCommand && ((SliceFromReadCommand)command).reversed;
                List<ColumnOrSuperColumn> thriftifiedColumns = ThriftConverter.thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, reverseOrder);
                thriftifiedColumnFamiliesMap.put(command.key, thriftifiedColumns);
            }
            return new ThriftifiedSliceMap(thriftifiedColumnFamiliesMap);
        } else {
            Map<ByteBuffer, Collection<IColumn>> columnFamiliesMap = new HashMap<ByteBuffer, Collection<IColumn>>();
            for (ReadCommand command: commands)
            {
                ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(command.key));
                columnFamiliesMap.put(command.key, cf.getSortedColumns());
            }
            return new InternalSliceMap(columnFamiliesMap);
        }
    }

    @Override
    public GetSliceResult get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("get_slice");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        List<ColumnOrSuperColumn> result = multigetSliceInternal(state().getKeyspace(), Collections.singletonList(key), column_parent, predicate, consistency_level).get(key);
        if (logger.isTraceEnabled()) {
            logger.trace("get_slice({}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_parent, predicate, consistency_level, lts, result});
        }
        return new GetSliceResult(result, LamportClock.sendTimestamp());
    }

    @Override
    public MultigetSliceResult multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("multiget_slice");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = multigetSliceInternal(state().getKeyspace(), keys, column_parent, predicate, consistency_level);
        if (logger.isTraceEnabled()) {
            logger.trace("multiget_slice({}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.listBytesToHex(keys), column_parent, predicate, consistency_level, lts, result});
        }
        return new MultigetSliceResult(result, LamportClock.sendTimestamp());
    }


    private void selectChosenResults(Map<ByteBuffer, Collection<IColumn>> keyToColumnFamily, SlicePredicate predicate, long chosen_time, Map<ByteBuffer, List<ColumnOrSuperColumn>> keyToChosenColumns, Set<Long> pendingTransactionIds)
    {
        for(Entry<ByteBuffer, Collection<IColumn>> entry : keyToColumnFamily.entrySet()) {
            ByteBuffer key = entry.getKey();
            Collection<IColumn> columns = entry.getValue();

            List<ColumnOrSuperColumn> chosenColumns = new ArrayList<ColumnOrSuperColumn>();
            for (IColumn column : columns) {
                ChosenColumnResult ccr = ThriftConverter.selectChosenColumn(column, chosen_time);
                if (ccr.pendingTransaction) {
                    pendingTransactionIds.addAll(ccr.transactionIds);
                } else {
                    chosenColumns.add(ccr.cosc);
                }
            }

            if (predicate.isSetSlice_range() && predicate.slice_range.reversed) {
                Collections.reverse(chosenColumns);
            }

            keyToChosenColumns.put(key, chosenColumns);
        }
    }

    @Override
    public MultigetSliceResult multiget_slice_by_time(List<ByteBuffer> keys, ColumnParent column_parent,
    SlicePredicate predicate, ConsistencyLevel consistency_level, long chosen_time, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("multiget_slice_by_time");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);

        //do the multiget but dont convert to thrift so we can still access the previous values
        ISliceMap iSliceMap = multigetSliceInternal(state().getKeyspace(), keys, column_parent, predicate, consistency_level, false);
        assert iSliceMap instanceof InternalSliceMap : "thriftified was false, so it should be an internal map";
        Map<ByteBuffer, Collection<IColumn>> keyToColumnFamily = ((InternalSliceMap) iSliceMap).cassandraMap;

        //select results for each key that were visible at the chosen_time
        Map<ByteBuffer, List<ColumnOrSuperColumn>> keyToChosenColumns = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
        Set<Long> pendingTransactionIds = new HashSet<Long>();
        selectChosenResults(keyToColumnFamily, predicate, chosen_time, keyToChosenColumns, pendingTransactionIds);

        if (pendingTransactionIds.size() > 0) {
            //There are pending transactions, we need to determine if they've happened yet and then compute the real result
            try {
                TransactionProxy.checkTransactions(state().getKeyspace(), pendingTransactionIds, chosen_time);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            keyToChosenColumns = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
            pendingTransactionIds = new HashSet<Long>();
            selectChosenResults(keyToColumnFamily, predicate, chosen_time, keyToChosenColumns, pendingTransactionIds);
            assert pendingTransactionIds.size() == 0 : "Should have resolved all pending transaction";
        }

        if (DatabaseDescriptor.isForcedByTimeReadIndirection() && pendingTransactionIds.size() == 0) {
            logger.trace("Forcing an indirection on a multiget_slice_by_time.");

            try {
                TransactionProxy.forceCheckTransaction(state().getKeyspace(), chosen_time);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("multiget_slice_by_time({}, {}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.listBytesToHex(keys), column_parent, predicate, consistency_level, chosen_time, lts, keyToChosenColumns});
        }
        return new MultigetSliceResult(keyToChosenColumns, LamportClock.sendTimestamp());
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ISliceMap iSliceMap = multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level, true);
        assert iSliceMap instanceof ThriftifiedSliceMap : "thriftied was true, so this should be a thrifitied map";
        return ((ThriftifiedSliceMap) iSliceMap).thriftifiedMap;
    }

    private ISliceMap multigetSliceInternal(String keyspace, List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, boolean thriftify)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        if (predicate.column_names != null)
        {
            for (ByteBuffer key: keys)
            {
                ThriftValidation.validateKey(metadata, key);
                commands.add(new SliceByNamesReadCommand(keyspace, key, column_parent, predicate.column_names));
            }
        }
        else
        {
            SliceRange range = predicate.slice_range;
            for (ByteBuffer key: keys)
            {
                ThriftValidation.validateKey(metadata, key);
                commands.add(new SliceFromReadCommand(keyspace, key, column_parent, range.start, range.finish, range.reversed, range.count));
            }
        }

        return getSlice(commands, consistency_level, thriftify);
    }



    private ColumnOrSuperColumn internal_get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        state().hasColumnFamilyAccess(column_path.column_family, Permission.READ);
        String keyspace = state().getKeyspace();

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family);
        ThriftValidation.validateColumnPath(metadata, column_path);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        QueryPath path = new QueryPath(column_path.column_family, column_path.column == null ? null : column_path.super_column);
        List<ByteBuffer> nameAsList = Arrays.asList(column_path.column == null ? column_path.super_column : column_path.column);
        ThriftValidation.validateKey(metadata, key);
        ReadCommand command = new SliceByNamesReadCommand(keyspace, key, path, nameAsList);

        Map<DecoratedKey, ColumnFamily> cfamilies = readColumnFamily(Arrays.asList(command), consistency_level);

        ColumnFamily cf = cfamilies.get(StorageService.getPartitioner().decorateKey(command.key));

        if (cf == null)
            throw new NotFoundException();
        List<ColumnOrSuperColumn> tcolumns = ThriftConverter.thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, false);
        if (tcolumns.isEmpty())
            throw new NotFoundException();
        assert tcolumns.size() == 1;
        return tcolumns.get(0);
    }

    @Override
    public GetResult get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("get");

        ColumnOrSuperColumn result = internal_get(key, column_path, consistency_level);
        if (logger.isTraceEnabled()) {
            logger.trace("get({}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_path, consistency_level, lts, result});
        }
        return new GetResult(result, LamportClock.sendTimestamp());
    }

    @Override
    public GetCountResult get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("get_count");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        Table table = Table.open(state().getKeyspace());
        ColumnFamilyStore cfs = table.getColumnFamilyStore(column_parent.column_family);

        if (predicate.column_names != null) {
            GetSliceResult result = get_slice(key, column_parent, predicate, consistency_level, LamportClock.NO_CLOCK_TICK);
            //we use a ClientContext to simplify determining dependencies
            //filter out deleted columns, but keep dependencies on them
            ClientContext countContext = new ClientContext();
            for (Iterator<ColumnOrSuperColumn> cosc_it = result.value.iterator(); cosc_it.hasNext(); ) {
                ColumnOrSuperColumn cosc = cosc_it.next();
                try {
                    countContext.addDep(key, cosc);
                } catch (NotFoundException nfe) {
                    cosc_it.remove();
                }
            }
            return new GetCountResult(result.value.size(), countContext.getDeps(), LamportClock.sendTimestamp());
        }

        int pageSize;
        // request by page if this is a large row
        if (cfs.getMeanColumns() > 0)
        {
            int averageColumnSize = (int) (cfs.getMeanRowSize() / cfs.getMeanColumns());
            pageSize = Math.min(COUNT_PAGE_SIZE,
                                DatabaseDescriptor.getInMemoryCompactionLimit() / averageColumnSize);
            pageSize = Math.max(2, pageSize);
            logger.debug("average row column size is {}; using pageSize of {}", averageColumnSize, pageSize);
        }
        else
        {
            pageSize = COUNT_PAGE_SIZE;
        }

        int totalCount = 0;
        List<ColumnOrSuperColumn> columns;

        if (predicate.slice_range == null)
        {
            predicate.slice_range = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                   false,
                                                   Integer.MAX_VALUE);
        }

        //we use a ClientContext to simplify determining dependencies
        ClientContext countContext = new ClientContext();
        int requestedCount = predicate.slice_range.count;
        while (true)
        {
            predicate.slice_range.count = Math.min(pageSize, requestedCount);
            GetSliceResult result = get_slice(key, column_parent, predicate, consistency_level, LamportClock.NO_CLOCK_TICK);
            if (result.value.isEmpty())
                break;

            ColumnOrSuperColumn lastColumn = result.value.get(result.value.size() - 1);

            //filter out deleted columns, but keep dependencies on them
            boolean lastResultDeleted = false;
            for (Iterator<ColumnOrSuperColumn> cosc_it = result.value.iterator(); cosc_it.hasNext(); ) {
                ColumnOrSuperColumn cosc = cosc_it.next();
                try {
                    countContext.addDep(key, cosc);
                    lastResultDeleted = false;
                } catch (NotFoundException nfe) {
                    cosc_it.remove();
                    lastResultDeleted = true;
                }
            }

            columns = result.value;

            totalCount += columns.size();
            requestedCount -= columns.size();
            ByteBuffer lastName =
                    lastColumn.isSetSuper_column() ? lastColumn.super_column.name :
                        (lastColumn.isSetColumn() ? lastColumn.column.name :
                            (lastColumn.isSetCounter_column() ? lastColumn.counter_column.name : lastColumn.counter_super_column.name));
            if ((requestedCount == 0) || ((columns.size() <= 1) && (lastName.equals(predicate.slice_range.start))))
            {
                break;
            }
            else
            {
                predicate.slice_range.start = lastName;
                // remove the count for the column that starts the next slice (unless it's a deleted result)
                if (!lastResultDeleted) {
                    totalCount--;
                    requestedCount++;
                }
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("get_count({}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_parent, predicate, consistency_level, lts, totalCount});
        }
        return new GetCountResult(totalCount, countContext.getDeps(), LamportClock.sendTimestamp());
    }

    @Override
    public MultigetCountResult multiget_count_by_time(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long chosen_time, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        assert false : "Still in progress";
        return null;

//        LamportClock.updateTime(lts);
//        logger.debug("multiget_count_by_time");
//
//        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
//        String keyspace = state().getKeyspace();
//
//        Map<ByteBuffer, CountWithMetadata> results = new HashMap<ByteBuffer, CountWithMetadata>();
//
//        //do the multiget but dont convert to thrift so we can still access the previous values
//        ISliceMap iSliceMap = multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level, false);
//        assert iSliceMap instanceof InternalSliceMap : "thriftified was false, so it should be an internal map";
//        Map<ByteBuffer, Collection<IColumn>> keyToColumnFamily = ((InternalSliceMap) iSliceMap).cassandraMap;
//
//        //select results for each key that were visible at the chosen_time
//        for(Entry<ByteBuffer, Collection<IColumn>> entry : keyToColumnFamily.entrySet()) {
//            ByteBuffer key = entry.getKey();
//            Collection<IColumn> columns = entry.getValue();
//
//            //excludes deleted columns from the count; calculates dependencies (including deleted columns), evt, and lvt
//            ClientContext countContext = new ClientContext(); //use a clientContext to simplify calculating deps
//            long maxEarliestValidTime = Long.MIN_VALUE;
//            long minLatestValidTime = Long.MAX_VALUE;
//            int count = 0;
//
//            List<ColumnOrSuperColumn> chosenColumns = new ArrayList<ColumnOrSuperColumn>();
//            for (IColumn column : columns) {
//                ColumnOrSuperColumn cosc = ThriftConverter.selectChosenColumn(column, chosen_time);
//                EvtAndLvt evtAndLvt = ColumnOrSuperColumnHelper.extractEvtAndLvt(cosc);
//                maxEarliestValidTime = Math.max(maxEarliestValidTime, evtAndLvt.getEarliestValidTime());
//                minLatestValidTime = Math.min(minLatestValidTime, evtAndLvt.getLatestValidTime());
//                try {
//                    countContext.addDep(key, cosc);
//                    count++;
//                } catch (NotFoundException nfe) {
//                    //TODO: Don't use exceptions for this, have a separate addDep function for serverside use
//                    //don't increment count
//                }
//            }
//
//            results.put(key, new CountWithMetadata(count, maxEarliestValidTime, minLatestValidTime, countContext.getDeps()));
//        }
//
//        if (logger.isTraceEnabled()) {
//            logger.trace("multiget_count_by_time({}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.listBytesToHex(keys), column_parent, predicate, consistency_level, lts, results});
//        }
//        return new MultigetCountResult(results, LamportClock.sendTimestamp());
    }

    @Override
    public MultigetCountResult multiget_count(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("multiget_count");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        String keyspace = state().getKeyspace();

        Map<ByteBuffer, CountWithMetadata> results = new HashMap<ByteBuffer, CountWithMetadata>();
        Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level);

        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> cf : columnFamiliesMap.entrySet()) {
            //excludes deleted columns from the count; calculates dependencies (including deleted columns), evt, and lvt
            ClientContext countContext = new ClientContext(); //use a clientContext to simplify calculating deps
            long maxEarliestValidTime = Long.MIN_VALUE;
            long minLatestValidTime = Long.MAX_VALUE;
            for (Iterator<ColumnOrSuperColumn> cosc_it = cf.getValue().iterator(); cosc_it.hasNext(); ) {
                ColumnOrSuperColumn cosc = cosc_it.next();
                EvtAndLvt evtAndLvt = ColumnOrSuperColumnHelper.extractEvtAndLvt(cosc);
                maxEarliestValidTime = Math.max(maxEarliestValidTime, evtAndLvt.getEarliestValidTime());
                minLatestValidTime = Math.min(minLatestValidTime, evtAndLvt.getLatestValidTime());
                try {
                    countContext.addDep(cf.getKey(), cosc);
                } catch (NotFoundException nfe) {
                    cosc_it.remove();
                }
            }

            results.put(cf.getKey(), new CountWithMetadata(cf.getValue().size(), maxEarliestValidTime, minLatestValidTime, countContext.getDeps()));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("multiget_count({}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.listBytesToHex(keys), column_parent, predicate, consistency_level, lts, results});
        }
        return new MultigetCountResult(results, LamportClock.sendTimestamp());
    }

    private void internal_insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level, Set<Dep> deps)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        state().hasColumnFamilyAccess(column_parent.column_family, Permission.WRITE);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(state().getKeyspace(), column_parent.column_family, false);
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        // SuperColumn field is usually optional, but not when we're inserting
        if (metadata.cfType == ColumnFamilyType.Super && column_parent.super_column == null)
        {
            throw new InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);
        }
        ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));
        ThriftValidation.validateColumnData(metadata, column, column_parent.super_column != null);

        //TODO this thrift-related function will only be called at the accepting datacenter, don't need this check
        // At the accepting (local) datacenter, the timestamp (version) should
        // be 0 when sent to us, and we'll set it here.
        if (column.timestamp == 0) {
            column.timestamp = LamportClock.getVersion();
            logger.debug("Setting timestamp to {}", column.timestamp);
        }

        Set<Dependency> dependencies = new HashSet<Dependency>();
        for (Dep dep : deps) {
            dependencies.add(new Dependency(dep));
        }
        RowMutation rm = new RowMutation(state().getKeyspace(), key, dependencies);
        try
        {
            rm.add(new QueryPath(column_parent.column_family, column_parent.super_column, column.name), column.value, column.timestamp, column.ttl);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
        doInsert(consistency_level, Arrays.asList(rm));
    }

    @Override
    public WriteResult insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("insert");

        internal_insert(key, column_parent, column, consistency_level, deps);
        assert column.timestamp != 0 : "Column timestamp must have been set by now";
        if (logger.isTraceEnabled()) {
            logger.trace("insert({}, {}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_parent, column, consistency_level, deps, lts, column.timestamp});
        }
        return new WriteResult(column.timestamp, LamportClock.sendTimestamp());
    }

    private Set<Dep> internal_batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level, Set<Dep> deps)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        List<String> cfamsSeen = new ArrayList<String>();
        List<IMutation> rowMutations = new ArrayList<IMutation>();
        String keyspace = state().getKeyspace();

        Set<Dependency> dependencies = new HashSet<Dependency>();
        for (Dep dep : deps) {
            dependencies.add(new Dependency(dep));
        }

        //Note, we're assuming here the entire mutation is resident on this node, (all storageProxy calls are local)
        //the returnDeps are what we return to the client, for them to depend on after this call
        HashSet<Dep> returnDeps = new HashSet<Dep>();
        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet())
        {
            ByteBuffer key = mutationEntry.getKey();

            //Get the timestamp for the entire row mutation(s) (these are applied atomically in Cassandra 1)
            long opTimestamp = LamportClock.getVersion();
            returnDeps.add(new Dep(key, opTimestamp));

            // We need to separate row mutation for standard cf and counter cf (that will be encapsulated in a
            // CounterMutation) because it doesn't follow the same code path
            RowMutation rmStandard = null;
            RowMutation rmCounter = null;

            Map<String, List<Mutation>> columnFamilyToMutations = mutationEntry.getValue();
            for (Map.Entry<String, List<Mutation>> columnFamilyMutations : columnFamilyToMutations.entrySet())
            {
                String cfName = columnFamilyMutations.getKey();

                // Avoid unneeded authorizations
                if (!(cfamsSeen.contains(cfName)))
                {
                    state().hasColumnFamilyAccess(cfName, Permission.WRITE);
                    cfamsSeen.add(cfName);
                }

                CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
                ThriftValidation.validateKey(metadata, key);

                RowMutation rm;
                if (metadata.getDefaultValidator().isCommutative())
                {
                    ThriftValidation.validateCommutativeForWrite(metadata, consistency_level);
                    rmCounter = rmCounter == null ? new RowMutation(keyspace, key, dependencies) : rmCounter;
                    rm = rmCounter;
                }
                else
                {
                    rmStandard = rmStandard == null ? new RowMutation(keyspace, key, dependencies) : rmStandard;
                    rm = rmStandard;
                }

                for (Mutation mutation : columnFamilyMutations.getValue())
                {
                    ThriftValidation.validateMutation(metadata, mutation);

                    if (mutation.deletion != null)
                    {
                        rm.deleteColumnOrSuperColumn(cfName, mutation.deletion, opTimestamp, opTimestamp);
                    }
                    if (mutation.column_or_supercolumn != null)
                    {
                        rm.addColumnOrSuperColumn(cfName, mutation.column_or_supercolumn, opTimestamp, opTimestamp);
                    }
                }
            }
            if (rmStandard != null && !rmStandard.isEmpty())
                rowMutations.add(rmStandard);
            if (rmCounter != null && !rmCounter.isEmpty())
                rowMutations.add(new org.apache.cassandra.db.CounterMutation(rmCounter, consistency_level));
        }

        doInsert(consistency_level, rowMutations);

        return returnDeps;
    }

    @Override
    public BatchMutateResult batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("batch_mutate");

        Set<Dep> new_deps = internal_batch_mutate(mutation_map, consistency_level, deps);
        if (logger.isTraceEnabled()) {
            logger.trace("batch_mutate({}, {}, {}, {}) = {}", new Object[]{mutation_map, consistency_level, deps, lts, new_deps});
        }
        return new BatchMutateResult(new_deps, LamportClock.sendTimestamp());
    }

    private long internal_remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level, Set<Dep> deps, boolean isCommutativeOp)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        state().hasColumnFamilyAccess(column_path.column_family, Permission.WRITE);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(state().getKeyspace(), column_path.column_family, isCommutativeOp);
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnPathOrParent(metadata, column_path);
        if (isCommutativeOp)
            ThriftValidation.validateCommutativeForWrite(metadata, consistency_level);

        // At the accepting (local) datacenter, the timestamp (version) should
        // be 0 when sent to us, and we'll set it here.
        if (timestamp == 0) {
            timestamp = LamportClock.getVersion();
            logger.debug("Setting timestamp to {}", timestamp);
        }

        Set<Dependency> dependencies = new HashSet<Dependency>();
        for (Dep dep : deps) {
            dependencies.add(new Dependency(dep));
        }

        RowMutation rm = new RowMutation(state().getKeyspace(), key, dependencies);
        rm.delete(new QueryPath(column_path), timestamp);

        if (isCommutativeOp)
            doInsert(consistency_level, Arrays.asList(new CounterMutation(rm, consistency_level)));
        else
            doInsert(consistency_level, Arrays.asList(rm));

        return timestamp;
    }

    @Override
    public WriteResult remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("remove");

        long remove_timestamp = internal_remove(key, column_path, timestamp, consistency_level, deps, false);
        if (timestamp == 0) {
            assert remove_timestamp > 0;
        } else {
            assert timestamp == remove_timestamp;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("remove({}, {}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_path, timestamp, consistency_level, deps, lts, remove_timestamp});
        }
        return new WriteResult(remove_timestamp, LamportClock.sendTimestamp());
    }

    private void doInsert(ConsistencyLevel consistency_level, List<? extends IMutation> mutations) throws UnavailableException, TimedOutException, InvalidRequestException
    {
        ThriftValidation.validateConsistencyLevel(state().getKeyspace(), consistency_level, RequestType.WRITE);
        if (mutations.isEmpty())
            return;
        try
        {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                StorageProxy.mutate(mutations, consistency_level);
            }
            finally
            {
                release();
            }
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
    }

    @Override
    public KsDef describe_keyspace(String table) throws NotFoundException, InvalidRequestException
    {
        state().hasKeyspaceSchemaAccess(Permission.READ);

        KSMetaData ksm = Schema.instance.getTableDefinition(table);
        if (ksm == null)
            throw new NotFoundException();

        return ksm.toThrift();
    }

    private List<KeySlice> getRangeSlicesInternal(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, List<ByteBuffer> knownKeys, ConsistencyLevel consistency_level, Long chosen_time)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        String keyspace = state().getKeyspace();
        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);
        ThriftValidation.validateKeyRange(metadata, column_parent.super_column, range);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        List<Row> rows;
        try
        {
            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds;
            if (range.start_key == null)
            {
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right, p);
            }
            else
            {
                bounds = new Bounds<RowPosition>(RowPosition.forKey(range.start_key, p), RowPosition.forKey(range.end_key, p));
            }
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_parent, predicate, bounds, range.row_filter, range.count), consistency_level);
            }
            finally
            {
                release();
            }
            assert rows != null;
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        //filter out knownKeys
        for (Iterator<Row> row_it = rows.iterator(); row_it.hasNext(); ) {
            Row row = row_it.next();
            if (knownKeys.contains(row.key.key)) {
                row_it.remove();
            }
        }

        List<KeySlice> result;
        if (chosen_time == null) {
            result = ThriftConverter.thriftifyKeySlices(rows, column_parent, predicate);
        } else {
            result = ThriftConverter.thriftifyKeySlicesAtTime(rows, column_parent, predicate, chosen_time);
        }
        return result;

    }

    @Override
    public GetRangeSlicesResult get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("range_slice");

        List<KeySlice> result = getRangeSlicesInternal(column_parent, predicate, range, Collections.<ByteBuffer> emptyList(), consistency_level, null);

        if (logger.isTraceEnabled()) {
            logger.trace("get_range_slices({}, {}, {}, {}, {}) = {}", new Object[]{column_parent, predicate, range, consistency_level, lts, result});
        }
        return new GetRangeSlicesResult(result, LamportClock.sendTimestamp());
    }

    @Override
    public GetRangeSlicesResult get_range_slices_by_time(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, List<ByteBuffer> knownKeys, ConsistencyLevel consistency_level, long chosen_time, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("range_slice_by_time");

        List<KeySlice> result = getRangeSlicesInternal(column_parent, predicate, range, knownKeys, consistency_level, chosen_time);

        if (logger.isTraceEnabled()) {
            logger.trace("get_range_slices({}, {}, {}, {}, {}) = {}", new Object[]{column_parent, predicate, range, consistency_level, lts, result});
        }
        return new GetRangeSlicesResult(result, LamportClock.sendTimestamp());
    }

    @Override
    public GetIndexedSlicesResult get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level, long lts) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("scan");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        String keyspace = state().getKeyspace();
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, column_predicate);
        ThriftValidation.validateIndexClauses(metadata, index_clause);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        List<Row> rows;
        try
        {
            rows = StorageProxy.scan(keyspace,
                                     column_parent.column_family,
                                     index_clause,
                                     column_predicate,
                                     consistency_level);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        return new GetIndexedSlicesResult(ThriftConverter.thriftifyKeySlices(rows, column_parent, column_predicate), LamportClock.sendTimestamp());
    }

    @Override
    public List<KsDef> describe_keyspaces() throws TException, InvalidRequestException
    {
        state().hasKeyspaceSchemaAccess(Permission.READ);

        Set<String> keyspaces = Schema.instance.getTables();
        List<KsDef> ksset = new ArrayList<KsDef>();
        for (String ks : keyspaces)
        {
            try
            {
                ksset.add(describe_keyspace(ks));
            }
            catch (NotFoundException nfe)
            {
                logger.info("Failed to find metadata for keyspace '" + ks + "'. Continuing... ");
            }
        }
        return ksset;
    }

    @Override
    public String describe_cluster_name() throws TException
    {
        return DatabaseDescriptor.getClusterName();
    }

    @Override
    public String describe_version() throws TException
    {
        return Constants.VERSION;
    }

    @Override
    public List<TokenRange> describe_ring(String keyspace)throws InvalidRequestException
    {
        return StorageService.instance.describeRing(keyspace);
    }

    @Override
    public String describe_partitioner() throws TException
    {
        return StorageService.getPartitioner().getClass().getName();
    }

    @Override
    public String describe_snitch() throws TException
    {
        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch)
            return ((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch.getClass().getName();
        return DatabaseDescriptor.getEndpointSnitch().getClass().getName();
    }

    @Override
    public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split)
    throws TException, InvalidRequestException
    {
        // TODO: add keyspace authorization call post CASSANDRA-1425
        Token.TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = StorageService.instance.getSplits(state().getKeyspace(), cfName, new Range<Token>(tf.fromString(start_token), tf.fromString(end_token)), keys_per_split);
        List<String> splits = new ArrayList<String>(tokens.size());
        for (Token token : tokens)
        {
            splits.add(tf.toString(token));
        }
        return splits;
    }

    @Override
    public long login(AuthenticationRequest auth_request, long lts) throws AuthenticationException, AuthorizationException, TException
    {
        LamportClock.updateTime(lts);
        state().login(auth_request.getCredentials());
        return LamportClock.sendTimestamp();
    }

    /**
     * Schedule the current thread for access to the required services
     */
    protected void schedule(long timeoutMS) throws TimeoutException
    {
        requestScheduler.queue(Thread.currentThread(), state().getSchedulingValue(), timeoutMS);
    }

    /**
     * Release count for the used up resources
     */
    protected void release()
    {
        requestScheduler.release();
    }

    // helper method to apply migration on the migration stage. typical migration failures will throw an
    // InvalidRequestException. atypical failures will throw a RuntimeException.
    private static void applyMigrationOnStage(final Migration m)
    {
        Future f = StageManager.getStage(Stage.MIGRATION).submit(new Callable()
        {
            @Override
            public Object call() throws Exception
            {
                m.apply();
                m.announce();
                return null;
            }
        });
        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized String system_add_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("add_column_family");
        state().hasColumnFamilySchemaAccess(Permission.WRITE);
        CFMetaData.addDefaultIndexNames(cf_def);
        ThriftValidation.validateCfDef(cf_def, null);
        validateSchemaAgreement();

        try
        {
            cf_def.unsetId(); // explicitly ignore any id set by client (Hector likes to set zero)
            applyMigrationOnStage(new AddColumnFamily(CFMetaData.fromThrift(cf_def)));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_drop_column_family(String column_family)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("drop_column_family");
        state().hasColumnFamilySchemaAccess(Permission.WRITE);
        validateSchemaAgreement();

        try
        {
            applyMigrationOnStage(new DropColumnFamily(state().getKeyspace(), column_family));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_add_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("add_keyspace");
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        validateSchemaAgreement();
        ThriftValidation.validateKeyspaceNotYetExisting(ks_def.name);

        // generate a meaningful error if the user setup keyspace and/or column definition incorrectly
        for (CfDef cf : ks_def.cf_defs)
        {
            if (!cf.getKeyspace().equals(ks_def.getName()))
            {
                throw new InvalidRequestException("CfDef (" + cf.getName() +") had a keyspace definition that did not match KsDef");
            }
        }

        try
        {
            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(ks_def.cf_defs.size());
            for (CfDef cf_def : ks_def.cf_defs)
            {
                cf_def.unsetId(); // explicitly ignore any id set by client (same as system_add_column_family)
                CFMetaData.addDefaultIndexNames(cf_def);
                ThriftValidation.validateCfDef(cf_def, null);
                cfDefs.add(CFMetaData.fromThrift(cf_def));
            }

            ThriftValidation.validateKsDef(ks_def);
            applyMigrationOnStage(new AddKeyspace(KSMetaData.fromThrift(ks_def, cfDefs.toArray(new CFMetaData[cfDefs.size()]))));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_drop_keyspace(String keyspace)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("drop_keyspace");
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        validateSchemaAgreement();

        try
        {
            applyMigrationOnStage(new DropKeyspace(keyspace));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    /** update an existing keyspace, but do not allow column family modifications.
     * @throws SchemaDisagreementException
     */
    @Override
    public synchronized String system_update_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("update_keyspace");
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        ThriftValidation.validateTable(ks_def.name);
        if (ks_def.getCf_defs() != null && ks_def.getCf_defs().size() > 0)
            throw new InvalidRequestException("Keyspace update must not contain any column family definitions.");
        validateSchemaAgreement();

        try
        {
            ThriftValidation.validateKsDef(ks_def);
            applyMigrationOnStage(new UpdateKeyspace(KSMetaData.fromThrift(ks_def)));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_update_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("update_column_family");
        state().hasColumnFamilySchemaAccess(Permission.WRITE);
        if (cf_def.keyspace == null || cf_def.name == null)
            throw new InvalidRequestException("Keyspace and CF name must be set.");
        CFMetaData oldCfm = Schema.instance.getCFMetaData(cf_def.keyspace, cf_def.name);
        if (oldCfm == null)
            throw new InvalidRequestException("Could not find column family definition to modify.");
        CFMetaData.addDefaultIndexNames(cf_def);
        ThriftValidation.validateCfDef(cf_def, oldCfm);
        validateSchemaAgreement();

        try
        {
            // ideally, apply() would happen on the stage with the
            CFMetaData.applyImplicitDefaults(cf_def);
            org.apache.cassandra.db.migration.avro.CfDef result;
            try
            {
                result = CFMetaData.fromThrift(cf_def).toAvro();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            UpdateColumnFamily update = new UpdateColumnFamily(result);
            applyMigrationOnStage(update);
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    private void validateSchemaAgreement() throws SchemaDisagreementException
    {
        // unreachable hosts don't count towards disagreement
        Map<String, List<String>> versions = Maps.filterKeys(StorageProxy.describeSchemaVersions(),
                                                             Predicates.not(Predicates.equalTo(StorageProxy.UNREACHABLE)));
        if (versions.size() > 1)
            throw new SchemaDisagreementException();
    }

    @Override
    public long truncate(String cfname, Set<Dep> deps, long lts) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);

        logger.debug("truncating {} in {}", cfname, state().getKeyspace());
        state().hasColumnFamilyAccess(cfname, Permission.WRITE);
        try
        {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                StorageProxy.truncateBlocking(state().getKeyspace(), cfname);
            }
            finally
            {
                release();
            }
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw (UnavailableException) new UnavailableException().initCause(e);
        }

        return LamportClock.sendTimestamp();
    }

    @Override
    public long set_keyspace(String keyspace, long lts) throws InvalidRequestException, TException
    {
        LamportClock.updateTime(lts);
        ThriftValidation.validateTable(keyspace);

        state().setKeyspace(keyspace);
        return LamportClock.sendTimestamp();
    }

    @Override
    public Map<String, List<String>> describe_schema_versions() throws TException, InvalidRequestException
    {
        logger.debug("checking schema agreement");
        return StorageProxy.describeSchemaVersions();
    }

    // counter methods

    @Override
    public WriteResult add(ByteBuffer key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("add");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.WRITE);
        String keyspace = state().getKeyspace();

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, true);
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateCommutativeForWrite(metadata, consistency_level);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        // SuperColumn field is usually optional, but not when we're adding
        if (metadata.cfType == ColumnFamilyType.Super && column_parent.super_column == null)
        {
            throw new InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);
        }
        ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));

        Set<Dependency> dependencies = new HashSet<Dependency>();
        for (Dep dep : deps) {
            dependencies.add(new Dependency(dep));
        }

        //TODO: Make this faster and more elegant
        //add operations also need a dependency on the previous value for this datacenter
        try {
            ColumnOrSuperColumn cosc = this.internal_get(key, new ColumnPath(column_parent.column_family).setSuper_column(column_parent.super_column).setColumn(column.name), ConsistencyLevel.ONE);
            if (cosc.isSetColumn()) {
                //WL TODO: Maybe allow looser use of countercolumn and then add a dep on the delete here
                //note, if the deleted time was set, then the countercolumn was deleted.  we don't need to depend
                //on that delete because clients have to wait until it reaches all nodes before resurrecting it
                assert cosc.column.isSetDeleted_time();
            } else {
                ClientContext tmpContext = new ClientContext();
                tmpContext.addDep(key, cosc);
                if (tmpContext.getDeps().size() > 0) {
                    Dependency newDep = new Dependency(tmpContext.getDeps().iterator().next());
                    dependencies.add(newDep);
                    logger.debug("Adding a dependency on the previous value from this dc: " + newDep);
                }
            }
        } catch (NotFoundException e1) {
            //this is fine, it's the first add for this datacenter, no dep needed
        }

        RowMutation rm = new RowMutation(keyspace, key, dependencies);
        long timestamp = LamportClock.getVersion();
        try
        {
            rm.addCounter(new QueryPath(column_parent.column_family, column_parent.super_column, column.name), column.value, timestamp, timestamp, null);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
        doInsert(consistency_level, Arrays.asList(new CounterMutation(rm, consistency_level)));

        if (logger.isTraceEnabled()) {
            logger.trace("add({}, {}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_parent, column, consistency_level, deps, lts, timestamp});
        }
        return new WriteResult(timestamp, LamportClock.sendTimestamp());
    }

    @Override
    public WriteResult remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        logger.debug("remove_counter");

        long remove_timestamp = internal_remove(key, path, LamportClock.getVersion(), consistency_level, deps, true);
        assert remove_timestamp > 0;
        if (logger.isTraceEnabled()) {
            logger.trace("remove_counter({}, {}, {}, {}, {}) = {}", new Object[]{key, path, consistency_level, deps, lts, remove_timestamp});
        }
        return new WriteResult(remove_timestamp, LamportClock.sendTimestamp());
    }

    private static String uncompress(ByteBuffer query, Compression compression) throws InvalidRequestException
    {
        String queryString = null;

        // Decompress the query string.
        try
        {
            switch (compression)
            {
                case GZIP:
                    FastByteArrayOutputStream byteArray = new FastByteArrayOutputStream();
                    byte[] outBuffer = new byte[1024], inBuffer = new byte[1024];

                    Inflater decompressor = new Inflater();

                    int lenRead = 0;
                    while (true)
                    {
                        if (decompressor.needsInput())
                            lenRead = query.remaining() < 1024 ? query.remaining() : 1024;
                            query.get(inBuffer, 0, lenRead);
                            decompressor.setInput(inBuffer, 0, lenRead);

                        int lenWrite = 0;
                        while ((lenWrite = decompressor.inflate(outBuffer)) !=0)
                            byteArray.write(outBuffer, 0, lenWrite);

                        if (decompressor.finished())
                            break;
                    }

                    decompressor.end();

                    queryString = new String(byteArray.toByteArray(), 0, byteArray.size(), "UTF-8");
                    break;
                case NONE:
                    try
                    {
                        queryString = ByteBufferUtil.string(query);
                    }
                    catch (CharacterCodingException ex)
                    {
                        throw new InvalidRequestException(ex.getMessage());
                    }
                    break;
            }
        }
        catch (DataFormatException e)
        {
            throw new InvalidRequestException("Error deflating query string.");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new InvalidRequestException("Unknown query string encoding.");
        }
        return queryString;
    }

    @Override
    public CqlResult execute_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        if (logger.isDebugEnabled()) logger.debug("execute_cql_query");

        String queryString = uncompress(query,compression);

        try
        {
            return QueryProcessor.process(queryString, state());
        }
        catch (RecognitionException e)
        {
            InvalidRequestException ire = new InvalidRequestException("Invalid or malformed CQL query string");
            ire.initCause(e);
            throw ire;
        }
    }

    @Override
    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, TException
    {
        if (logger.isDebugEnabled()) logger.debug("prepare_cql_query");

        String queryString = uncompress(query,compression);

        try
        {
            return QueryProcessor.prepare(queryString, state());
        }
        catch (RecognitionException e)
        {
            InvalidRequestException ire = new InvalidRequestException("Invalid or malformed CQL query string");
            ire.initCause(e);
            throw ire;
        }
    }

    @Override
    public CqlResult execute_prepared_cql_query(int itemId, List<String> bindVariables)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        if (logger.isDebugEnabled()) logger.debug("execute_prepared_cql_query");

        CQLStatement statement = state().getPrepared().get(itemId);

        if (statement == null)
            throw new InvalidRequestException(String.format("Prepared query with ID %d not found", itemId));
        logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, state().getPrepared().size());

        return QueryProcessor.processPrepared(statement, state(), bindVariables);
    }

    private void checkPermission(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map)
    throws InvalidRequestException
    {
        //check for permission
        List<String> cfamsSeen = new ArrayList<String>();

        for (Map<String, List<Mutation>> columnFamilyToMutations: mutation_map.values()) {
            for (String cfName : columnFamilyToMutations.keySet()) {
                // Avoid unneeded authorizations
                if (!(cfamsSeen.contains(cfName))) {
                    state().hasColumnFamilyAccess(cfName, Permission.WRITE);
                    cfamsSeen.add(cfName);
                }
            }
        }

        //WL TODO: Also do thrift validation here too so it throws errors at the correct place
    }

    @Override
    public short transactional_batch_mutate_cohort(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ByteBuffer coordinator_key, long transaction_id, long lts)
    throws InvalidRequestException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("transactional_batch_mutate_cohort");

        String keyspace;
        try {
            keyspace = state().getKeyspace();
            checkPermission(mutation_map);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }

        BatchMutateTransactionCohort cohort = new BatchMutateTransactionCohort();
        cohort.receiveLocalTransaction(keyspace, mutation_map, coordinator_key, transaction_id);

        return 0;
    }

    @Override
    public BatchMutateResult transactional_batch_mutate_coordinator(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ConsistencyLevel consistency_level, Set<Dep> deps, ByteBuffer coordinator_key, Set<ByteBuffer> all_keys, long transaction_id, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("transactional_batch_mutate_coordinator");

        ByteBuffer originalCoordinatorKey = ByteBufferUtil.hexToBytes(ByteBufferUtil.bytesToHex(coordinator_key));

        assert consistency_level == ConsistencyLevel.LOCAL_QUORUM || consistency_level == ConsistencyLevel.ONE;

        String keyspace = state().getKeyspace();
        checkPermission(mutation_map);
        Set<Dependency> dependencies = new HashSet<Dependency>();
        for (Dep dep : deps) {
            dependencies.add(new Dependency(dep));
        }

        BatchMutateTransactionCoordinator coordinator = new BatchMutateTransactionCoordinator();
        try {
            coordinator.receiveLocalTransaction(keyspace, mutation_map, dependencies, coordinator_key, all_keys, transaction_id);
        } catch (IOException e) {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        } catch (TimeoutException e) {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        long timestamp = coordinator.waitForLocalCommitNoInterruption();

        //the entire BMT is a single op, so it only create a single dependency
        Set<Dep> new_deps = Collections.singleton(new Dep(originalCoordinatorKey, timestamp));

        if (logger.isTraceEnabled()) {
            logger.trace("transactional_batch_mutate_coordinator({}, {}, {}, {}) = {}", new Object[]{mutation_map, consistency_level, deps, lts, new_deps});
        }
        return new BatchMutateResult(new_deps, LamportClock.sendTimestamp());
    }

    // main method moved to CassandraDaemon
}
