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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

public class DeletedColumn extends Column
{
    public DeletedColumn(ByteBuffer name, int localDeletionTime, long timestamp, ByteBuffer transactionCoordinatorKey)
    {
        this(name, ByteBufferUtil.bytes(localDeletionTime), timestamp, transactionCoordinatorKey);
    }

    public DeletedColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, null);
    }


    public DeletedColumn(ByteBuffer name, ByteBuffer value, long timestamp, ByteBuffer transactionCoordinatorKey)
    {
        //WL Notes:
        //localDeletionTime is the system time in seconds when the delete is issued
        //timestamp is the logical time of the delete
        this(name, value, timestamp, null, null, timestamp, null, null, transactionCoordinatorKey);
    }


    public DeletedColumn(ByteBuffer name, ByteBuffer value, long timestamp, Long lastAccessTime, Long previousVersionLastAccessTime, Long earliestValidTime, Long latestValidTime, NavigableSet<IColumn> previousVersions, ByteBuffer transactionCoordinatorKey)
    {
        super(name, value, timestamp, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey);
    }

    @Override
    public boolean isMarkedForDelete()
    {
        return true;
    }

    @Override
    public long getMarkedForDeleteAt()
    {
        return timestamp;
    }

    @Override
    public int getLocalDeletionTime()
    {
       return value.getInt(value.position());
    }

    @Override
    public IColumn reconcile(IColumn column, Allocator allocator)
    {
        if (column instanceof DeletedColumn)
            return super.reconcile(column, allocator);
        return column.reconcile(this, allocator);
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs)
    {
        return new DeletedColumn(cfs.internOrCopy(name, HeapAllocator.instance), ByteBufferUtil.clone(value), timestamp, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey);
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new DeletedColumn(cfs.internOrCopy(name, allocator), allocator.clone(value), timestamp, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey);
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.DELETION_MASK;
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        if (value().remaining() != 4)
            throw new MarshalException("A tombstone value should be 4 bytes long");
        if (getLocalDeletionTime() < 0)
            throw new MarshalException("The local deletion time should not be negative");
    }
}
