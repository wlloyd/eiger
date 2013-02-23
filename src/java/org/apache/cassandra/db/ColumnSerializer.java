package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.db.Column.EVTComparator;
import org.apache.cassandra.db.transaction.PendingTransactionColumn;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnSerializer implements IColumnSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnSerializer.class);

    public final static int DELETION_MASK       = 0x01;
    public final static int EXPIRATION_MASK     = 0x02;
    public final static int COUNTER_MASK        = 0x04;
    public final static int COUNTER_UPDATE_MASK = 0x08;
    public final static int PENDING_TRANSACTION_MASK = 0x10;

    @Override
    public void serialize(IColumn column, DataOutput dos)
    {
        assert column.name().remaining() > 0;
        ByteBufferUtil.writeWithShortLength(column.name(), dos);
        try
        {
            dos.writeByte(column.serializationFlags());
            if (column instanceof CounterColumn)
            {
                dos.writeLong(((CounterColumn)column).timestampOfLastDelete());
            }
            else if (column instanceof ExpiringColumn)
            {
                dos.writeInt(((ExpiringColumn) column).getTimeToLive());
                dos.writeInt(column.getLocalDeletionTime());
            }
            dos.writeLong(column.timestamp());
            ByteBufferUtil.writeWithLength(column.value(), dos);
            if (column instanceof Column)
            {
                Column col = (Column) column;
                dos.writeLong(col.lastAccessTime != null ? col.lastAccessTime : Long.MIN_VALUE);
                dos.writeLong(col.lastAccessTimeOfAPreviousVersion != null ? col.lastAccessTimeOfAPreviousVersion : Long.MIN_VALUE);
                dos.writeLong(col.earliestValidTime != null ? col.earliestValidTime : Long.MIN_VALUE);
                dos.writeLong(col.latestValidTime != null ? col.latestValidTime : Long.MIN_VALUE);
                if (col.previousVersions == null) {
                    dos.writeInt(-1);
                } else {
                    dos.writeInt(col.previousVersions.size());
                    synchronized (col) {
                        if (col.previousVersions != null) {
                            for (IColumn prevVersion : col.previousVersions) {
                                serialize(prevVersion, dos);
                            }
                        }
                    }
                }
                if (col.transactionCoordinatorKey != null) {
                    dos.writeInt(col.transactionCoordinatorKey.remaining());
                    ByteBufferUtil.writeWithLength(col.transactionCoordinatorKey, dos);
                } else {
                    dos.writeInt(0);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column deserialize(DataInput dis) throws IOException
    {
        return deserialize(dis, Flag.LOCAL);
    }

    /*
     * For counter columns, we must know when we deserialize them if what we
     * deserialize comes from a remote host. If it does, then we must clear
     * the delta.
     */
    public Column deserialize(DataInput dis, IColumnSerializer.Flag flag) throws IOException
    {
        return deserialize(dis, flag, (int) (System.currentTimeMillis() / 1000));
    }

    @Override
    public Column deserialize(DataInput dis, IColumnSerializer.Flag flag, int expireBefore) throws IOException
    {
        ByteBuffer name = ByteBufferUtil.readWithShortLength(dis);
        if (name.remaining() <= 0)
        {
            String format = "invalid column name length %d%s";
            String details = "";
            if (dis instanceof FileDataInput)
            {
                FileDataInput fdis = (FileDataInput)dis;
                details = String.format(" (%s, %d bytes remaining)", fdis.getPath(), fdis.bytesRemaining());
            }
            throw new CorruptColumnException(String.format(format, name.remaining(), details));
        }

        int b = dis.readUnsignedByte();
        if ((b & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = dis.readLong();
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
            Long lastAccessTime = dis.readLong();
            if (lastAccessTime == Long.MIN_VALUE) { lastAccessTime = null; }
            Long previousVersionLastAccessTime = dis.readLong();
            if (previousVersionLastAccessTime == Long.MIN_VALUE) { previousVersionLastAccessTime = null; }
            Long earliestValidTime = dis.readLong();
            if (earliestValidTime == Long.MIN_VALUE) { earliestValidTime = null; }
            Long latestValidTime = dis.readLong();
            if (latestValidTime == Long.MIN_VALUE) { latestValidTime = null; }
            NavigableSet<IColumn> previousVersions;
            int previousVersionsLength = dis.readInt();
            if (previousVersionsLength == -1) {
                previousVersions = null;
            } else {
                previousVersions = new TreeSet<IColumn>(new EVTComparator());
                for (int i = 0; i < previousVersionsLength; ++i) {
                    previousVersions.add(deserialize(dis));
                }
            }
            int transactionCoordinatorKeyLength = dis.readInt();
            ByteBuffer transactionCoordinatorKey = null;
            if (transactionCoordinatorKeyLength > 0) {
                ByteBufferUtil.readWithLength(dis);
            }
            return CounterColumn.create(name, value, ts, timestampOfLastDelete, flag, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions);
        }
        else if ((b & EXPIRATION_MASK) != 0)
        {
            int ttl = dis.readInt();
            int expiration = dis.readInt();
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);

            Long lastAccessTime = dis.readLong();
            if (lastAccessTime == Long.MIN_VALUE) { lastAccessTime = null; }
            Long previousVersionLastAccessTime = dis.readLong();
            if (previousVersionLastAccessTime == Long.MIN_VALUE) { previousVersionLastAccessTime = null; }
            Long earliestValidTime = dis.readLong();
            if (earliestValidTime == Long.MIN_VALUE) { earliestValidTime = null; }
            Long latestValidTime = dis.readLong();
            if (latestValidTime == Long.MIN_VALUE) { latestValidTime = null; }
            SortedSet<IColumn> previousVersions;
            int previousVersionsLength = dis.readInt();
            if (previousVersionsLength == -1) {
                previousVersions = null;
            } else {
                previousVersions = new TreeSet<IColumn>(new EVTComparator());
                for (int i = 0; i < previousVersionsLength; ++i) {
                    previousVersions.add(deserialize(dis));
                }
            }
            int transactionCoordinatorKeyLength = dis.readInt();
            ByteBuffer transactionCoordinatorKey = null;
            if (transactionCoordinatorKeyLength > 0) {
                ByteBufferUtil.readWithLength(dis);
            }
            return ExpiringColumn.create(name, value, ts, ttl, expiration, expireBefore, flag);
        }
        else
        {
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);

            Long lastAccessTime = dis.readLong();
            if (lastAccessTime == Long.MIN_VALUE) { lastAccessTime = null; }
            Long previousVersionLastAccessTime = dis.readLong();
            if (previousVersionLastAccessTime == Long.MIN_VALUE) { previousVersionLastAccessTime = null; }
            Long earliestValidTime = dis.readLong();
            if (earliestValidTime == Long.MIN_VALUE) { earliestValidTime = null; }
            Long latestValidTime = dis.readLong();
            if (latestValidTime == Long.MIN_VALUE) { latestValidTime = null; }
            NavigableSet<IColumn> previousVersions;
            int previousVersionsLength = dis.readInt();
            if (previousVersionsLength == -1) {
                previousVersions = null;
            } else {
                previousVersions = new TreeSet<IColumn>(new EVTComparator());
                for (int i = 0; i < previousVersionsLength; ++i) {
                    previousVersions.add(deserialize(dis));
                }
            }
            int transactionCoordinatorKeyLength = dis.readInt();
            ByteBuffer transactionCoordinatorKey = null;
            if (transactionCoordinatorKeyLength > 0) {
                ByteBufferUtil.readWithLength(dis);
            }


            return (b & COUNTER_UPDATE_MASK) != 0
                    ? new CounterUpdateColumn(name, value, ts, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey)
                    : ((b & DELETION_MASK) != 0)
                        ? new DeletedColumn(name, value, ts, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey)
                        : ((b & PENDING_TRANSACTION_MASK) != 0)
                            ? new PendingTransactionColumn(name, value, ts, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey)
                            : new Column(name, value, ts, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey);
        }
    }

    @Override
    public long serializedSize(IColumn object)
    {
        return object.serializedSize();
    }

    private static class CorruptColumnException extends IOException
    {
        public CorruptColumnException(String s)
        {
            super(s);
        }
    }
}
