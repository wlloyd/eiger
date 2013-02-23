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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.NavigableSet;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.utils.*;
import org.apache.log4j.Logger;

/**
 * A counter update while it hasn't been applied yet by the leader replica.
 *
 * Contains a single counter update. When applied by the leader replica, this
 * is transformed to a relevant CounterColumn. This Column is a temporary data
 * structure that should never be stored inside a memtable or an sstable.
 */
public class CounterUpdateColumn extends Column
{
    private static final Logger logger = Logger.getLogger(CounterUpdateColumn.class);

    public CounterUpdateColumn(ByteBuffer name, long value, long timestamp, ByteBuffer transactionCoordinatorKey)
    {
        this(name, ByteBufferUtil.bytes(value), timestamp, transactionCoordinatorKey);
    }

    public CounterUpdateColumn(ByteBuffer name, ByteBuffer value, long timestamp, ByteBuffer transactionCoordinatorKey)
    {
        super(name, value, timestamp, transactionCoordinatorKey);
        logger.trace("new CounterUpdateColumn(" + ByteBufferUtil.bytesToHex(name) + ", " + ByteBufferUtil.toLong(value) + ", " + VersionUtil.toString(timestamp) + " = " + this);
    }

    public CounterUpdateColumn(ByteBuffer name, ByteBuffer value, long timestamp, Long lastAccessTime, Long previousVersionLastAccessTime, Long earliestValidTime, Long latestValidTime, NavigableSet<IColumn> previousVersions, ByteBuffer transactionCoordinatorKey)
    {
        super(name, value, timestamp, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey);
    }

    public long delta()
    {
        return value().getLong(value().position());
    }

    @Override
    public IColumn diff(IColumn column)
    {
        // Diff is used during reads, but we should never read those columns
        throw new UnsupportedOperationException("This operation is unsupported on CounterUpdateColumn.");
    }

    @Override
    public IColumn reconcile(IColumn column, Allocator allocator)
    {
        // The only time this could happen is if a batchAdd ships two
        // increment for the same column. Hence we simply sums the delta.

        assert (column instanceof CounterUpdateColumn) || (column instanceof DeletedColumn) : "Wrong class type.";

        // tombstones take precedence
        if (column.isMarkedForDelete())
            return timestamp() > column.timestamp() ? this : column;

        // neither is tombstoned
        assert false : "haven't reason through this";
        CounterUpdateColumn c = (CounterUpdateColumn)column;
        ByteBuffer NOT_SURE_YET = null;
        return new CounterUpdateColumn(name(), delta() + c.delta(), Math.max(timestamp(), c.timestamp()), NOT_SURE_YET);
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_UPDATE_MASK;
    }

    @Override
    public CounterColumn localCopy(ColumnFamilyStore cfs)
    {
        return convertToCounterColumn(cfs, HeapAllocator.instance);
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return convertToCounterColumn(cfs, allocator);
    }

    private CounterColumn convertToCounterColumn(ColumnFamilyStore cfs, Allocator allocator)
    {
        //Update is converted when it's applied, so this is the right time to set the earliestValidTime
        long earliestValidTime;
        if (ShortNodeId.getLocalDC() == VersionUtil.extractDatacenter(timestamp)) {
            earliestValidTime = timestamp;
        } else {
            earliestValidTime = LamportClock.getVersion();
        }
        ByteBuffer coordinatorKey;
        if (this.transactionCoordinatorKey == null) {
            coordinatorKey = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        } else {
            coordinatorKey = this.transactionCoordinatorKey;
        }
        return new CounterColumn(cfs.internOrCopy(name, allocator),
                                 CounterContext.instance().create(delta(), timestamp(), coordinatorKey, allocator),
                                 timestamp(),
                                 Long.MIN_VALUE,
                                 null, null, earliestValidTime, null, null);
    }
}
