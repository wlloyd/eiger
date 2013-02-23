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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.transaction.PendingTransactionColumn;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.cassandra.utils.LamportClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Column is immutable, which prevents all kinds of confusion in a multithreaded environment.
 * (TODO: look at making SuperColumn immutable too.  This is trickier but is probably doable
 *  with something like PCollections -- http://code.google.com
 */

public class Column implements IColumn
{
    private static Logger logger = LoggerFactory.getLogger(Column.class);
    private static ColumnSerializer serializer = new ColumnSerializer();

    public static ColumnSerializer serializer()
    {
        return serializer;
    }

    protected final ByteBuffer name;
    protected final ByteBuffer value;
    protected long timestamp;

    protected Long lastAccessTime;
    protected Long lastAccessTimeOfAPreviousVersion;
    protected Long earliestValidTime;
    protected Long latestValidTime;
    // previousVersions should only be accessed when synchronized on the column
    // they are in. Avoid deadlocks by synchronizing on columns in evt order
    NavigableSet<IColumn> previousVersions;
    protected final ByteBuffer transactionCoordinatorKey;

    //protected final String constructionStackTrace;

    private final boolean sanityCheck = false;

    Column(ByteBuffer name)
    {
        this(name, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public Column(ByteBuffer name, ByteBuffer value)
    {
        this(name, value, 0);
    }

    public Column(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, null);
    }

    public Column(ByteBuffer name, ByteBuffer value, long timestamp, ByteBuffer transactionCoordinatorKey)
    {
        this(name, value, timestamp, null, null, null, null, null, transactionCoordinatorKey);
    }


    //sorts things so we see newest first
    protected static class EVTComparator implements Comparator<IColumn>
    {
        @Override
        public int compare(IColumn lhs, IColumn rhs)
        {
            if (lhs.earliestValidTime() > rhs.earliestValidTime()) {
                return -1;
            } else if (lhs.earliestValidTime() < rhs.earliestValidTime()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    public Column(ByteBuffer name, ByteBuffer value, long timestamp, Long lastAccessTime, Long lastAccessTimeOfAPreviousVersion, Long earliestValidTime, Long latestValidTime, NavigableSet<IColumn> previousVersions, ByteBuffer transactionCoordinatorKey)
    {
        assert name != null;
        assert value != null;
        assert name.remaining() <= IColumn.MAX_NAME_LENGTH;

        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
        this.lastAccessTime = lastAccessTime;
        this.lastAccessTimeOfAPreviousVersion = lastAccessTimeOfAPreviousVersion;
        this.earliestValidTime = earliestValidTime;
        this.latestValidTime = latestValidTime;
        if (previousVersions != null) {
            this.previousVersions = new TreeSet<IColumn>(new EVTComparator());
            this.previousVersions.addAll(previousVersions);
        } else {
            this.previousVersions = null;
        }
        this.transactionCoordinatorKey = transactionCoordinatorKey;

        // StringWriter stackTrace = new StringWriter();
        // new Throwable().printStackTrace(new PrintWriter(stackTrace));
        // constructionStackTrace = stackTrace.toString();
    }

    @Override
    public ByteBuffer name()
    {
        return name;
    }

    @Override
    public Column getSubColumn(ByteBuffer columnName)
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    @Override
    public ByteBuffer value()
    {
        this.lastAccessTime = System.currentTimeMillis();
        return value;
    }

    @Override
    public Collection<IColumn> getSubColumns()
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    @Override
    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    @Override
    public long timestamp()
    {
        return timestamp;
    }

    @Override
    public long maxTimestamp()
    {
        return timestamp;
    }

    @Override
    public boolean isMarkedForDelete()
    {
        return false;
    }

    @Override
    public long getMarkedForDeleteAt()
    {
        throw new IllegalStateException("column is not marked for delete");
    }

    @Override
    public long mostRecentLiveChangeAt()
    {
        return timestamp;
    }

    @Override
    public long lastAccessTime()
    {
        return lastAccessTime;
    }

    @Override
    public long lastAccessTimeOfAPreviousVersion()
    {
        return lastAccessTimeOfAPreviousVersion;
    }

    @Override
    public void setEarliestValidTime(long earliestValidTime)
    {
        //Sanity Check
        if (sanityCheck) {
            long currentTime = LamportClock.currentVersion();
            assert earliestValidTime <= currentTime : "Sanity Check: " + earliestValidTime + " !< " + currentTime + " for " + this; // + " construction: " + constructionStackTrace;
        }
        this.earliestValidTime = earliestValidTime;
    }

    @Override
    public long earliestValidTime()
    {
        if (sanityCheck) {
            assert earliestValidTime != null : "earliestValidTime must be set when a value is written which must be before now for " + toString(); // + " created by: " + constructionStackTrace;
            long currentTime = LamportClock.currentVersion();
            assert earliestValidTime <= currentTime : "Sanity Check: " + earliestValidTime + " !< " + currentTime + " for " + this; // + " construction: " + constructionStackTrace;
        }
        return earliestValidTime;
    }

    @Override
    public boolean isSetLatestValidTime()
    {
        return !(latestValidTime == null);
    }

    @Override
    public void setLatestValidTime(long latestValidTime)
    {
        //WL TODO: Probably need to synchronize on doing this?
        this.latestValidTime = latestValidTime;
    }

    @Override
    public long latestValidTime()
    {
        return latestValidTime;
    }

    //to be safe, any operation looking at previousVersions needs to synchronize on this column (in case previousVersions is null)
    @Override
    public NavigableSet<IColumn> previousVersions()
    {
        return previousVersions;
    }

    @Override
    public ByteBuffer transactionCoordinatorKey()
    {
        return transactionCoordinatorKey;
    }

    @Override
    public int size()
    {
        /*
         * Size of a column is =
         *   size of a name (short + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + 8 bytes for timestamp
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
         * + 4 longs
         * + 1 int and size of each of the previous versions
         * + 1 int + if set (transactionCoordinatorKey length + 1) for transactionCoordinatorKey
        */
        int size = DBConstants.shortSize + name.remaining() + 1 + DBConstants.tsSize + DBConstants.intSize + value.remaining() + 4*DBConstants.longSize + DBConstants.intSize;
        if (previousVersions != null) {
            for (IColumn previousVersion : previousVersions) {
                size += previousVersion.serializedSize();
            }
        }
        size += DBConstants.intSize + ((transactionCoordinatorKey == null) ? 0 : DBConstants.intSize + transactionCoordinatorKey.remaining());
        return size;
    }

    /*
     * This returns the size of the column when serialized.
     * @see com.facebook.infrastructure.db.IColumn#serializedSize()
    */
    @Override
    public int serializedSize()
    {
        return size();
    }

    @Override
    public int serializationFlags()
    {
        return 0;
    }

    @Override
    public void addColumn(IColumn column)
    {
        addColumn(null, null);
    }

    @Override
    public void addColumn(IColumn column, Allocator allocator)
    {
        throw new UnsupportedOperationException("This operation is not supported for simple columns.");
    }

    @Override
    public IColumn diff(IColumn column)
    {
        if (timestamp() < column.timestamp())
        {
            return column;
        }
        return null;
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.duplicate());
        digest.update(value.duplicate());

        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp);
            buffer.writeByte(serializationFlags());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public int getLocalDeletionTime()
    {
        throw new IllegalStateException("column is not marked for delete");
    }

    @Override
    public IColumn reconcile(IColumn column)
    {
        return reconcile(column, HeapAllocator.instance);
    }

    private void determineLatestValidTime(PendingTransactionColumn otherColumn)
    {
        //Must be < otherColumn.evt - 1, < this.lvt, >= this.evt

        if (this.latestValidTime != null) {
            this.latestValidTime = Math.min(otherColumn.earliestValidTime - 1, this.latestValidTime);
        } else {
            this.latestValidTime = otherColumn.earliestValidTime - 1;
        }

        this.latestValidTime = Math.max(this.latestValidTime, this.earliestValidTime);
    }

    @Override
    public IColumn reconcile(IColumn column, Allocator allocator)
    {
        assert column instanceof Column;
        Column otherColumn = (Column) column;

        if (timestamp == otherColumn.timestamp()) {
            //can't have ties in COPS, timestamps are globally unique, so these must be the same column
            return this;
        }

        //PendingTransactionColumns limit the latestValidTime of other columns
        if (this instanceof PendingTransactionColumn) {
            otherColumn.determineLatestValidTime((PendingTransactionColumn) this);
        }
        if (otherColumn instanceof PendingTransactionColumn) {
            this.determineLatestValidTime((PendingTransactionColumn) otherColumn);
        }

        if (this.timestamp() < otherColumn.timestamp()) {
            return otherColumn.updatePreviousVersion(this);
        } else {
            return this.updatePreviousVersion(otherColumn);
        }
    }

    //WL TODO Trigger this after the read txn timeout has passed
    private void removeOldPreviousVersions()
    {
        synchronized (this) {
            if (this.previousVersions == null) {
                return;
            }

            long safeTime = System.currentTimeMillis() - DatabaseDescriptor.getGetTransactionTimeoutInMs();

            Iterator<IColumn> prevVersionIt = this.previousVersions.iterator();
            while (prevVersionIt.hasNext()) {
                IColumn prevVersion = prevVersionIt.next();

                if (prevVersion.lastAccessTimeOfAPreviousVersion() < safeTime) {
                    prevVersionIt.remove();
                } else {
                    // lastAccessTimeOfAPreviousVersions are monotonically increasing so nothing else can be cleaned now
                    return;
                }
            }
        }
    }

    private void addPreviousVersion(Column previousColumn)
    {
        //WL TODO: Add just a minimal "OldColumn" here?

        synchronized (this) {
            if (this.previousVersions == null) {
                //TODO: could special case to reduce synchronization
                synchronized (previousColumn) {
                    if (previousColumn.previousVersions == null) {
                        this.previousVersions = new TreeSet<IColumn>(new EVTComparator());
                    } else {
                        previousColumn.removeOldPreviousVersions();
                        this.previousVersions = previousColumn.previousVersions;
                        previousColumn.previousVersions = null;
                    }
                }
                this.previousVersions.add(previousColumn);
            } else {
                this.previousVersions.add(previousColumn);
                //TODO: could special case to reduce synchronization
                synchronized (previousColumn) {
                    if (previousColumn.previousVersions != null) {
                        previousColumn.removeOldPreviousVersions();
                        this.previousVersions.addAll(previousColumn.previousVersions);
                        previousColumn.previousVersions = null;
                    }
                }
            }
        }
    }

    private Long nullSafeMax(Long a, Long b)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return a > b ? a : b;
    }

    protected Column updatePreviousVersion(Column previousColumn)
    {
        // Update lastAccessTimeOfAPreviousVersion
        this.lastAccessTimeOfAPreviousVersion = nullSafeMax(this.lastAccessTimeOfAPreviousVersion, previousColumn.lastAccessTime);
        this.lastAccessTimeOfAPreviousVersion = nullSafeMax(this.lastAccessTimeOfAPreviousVersion, previousColumn.lastAccessTimeOfAPreviousVersion);


        // Special Cases
        //TODO: Understand this HACK, added to avoid an occasional error during compaction, delete to make it reappear
        if (previousColumn.earliestValidTime == null) {
            return this;
        }

        //if either is a PendingTransactionColumn we keep both around
        if (this instanceof PendingTransactionColumn || previousColumn instanceof PendingTransactionColumn) {
            addPreviousVersion(previousColumn);
            return this;
        }

        if (previousColumn.earliestValidTime > this.earliestValidTime) {
            //previousColumn was just written, and was never visible, so we don't need to save it (this can happen during replication)

            //TODO: I'm uneasy this isn't true ...
            //assert previousColumn.lastAccessTime == null : previousColumn.lastAccessTime + " from " + previousColumn;

            return this;
        }


        // Normal Case: only save the old version if necessary
        if (previousColumn.lastAccessTimeOfAPreviousVersion != null &&
            previousColumn.lastAccessTimeOfAPreviousVersion > System.currentTimeMillis() - DatabaseDescriptor.getGetTransactionTimeoutInMs()) {
            //need to keep the older version for potential get_by_time
            addPreviousVersion(previousColumn);
            logger.debug("Saving an old version:" + previousColumn);
        } else {
            logger.debug("NOT saving an old version:" + previousColumn +
                    " because pC.lATOAPV = " + previousColumn.lastAccessTimeOfAPreviousVersion +
		    " vs " + (System.currentTimeMillis() - DatabaseDescriptor.getGetTransactionTimeoutInMs()) +
                    " pC.pVs.size() = " + (previousColumn.previousVersions != null ? previousColumn.previousVersions.size() : "null"));
            assert previousColumn.previousVersions == null ||
		previousColumn.previousVersions.size() == 0 ||
		previousColumn.lastAccessTimeOfAPreviousVersion <= System.currentTimeMillis() - DatabaseDescriptor.getGetTransactionTimeoutInMs();
        }

        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Column column = (Column)o;

        if (timestamp != column.timestamp)
            return false;
        if (!name.equals(column.name))
            return false;

        return value.equals(column.value);
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int)(timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs)
    {
        return localCopy(cfs, HeapAllocator.instance);
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new Column(cfs.internOrCopy(name, allocator), allocator.clone(value), timestamp, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions, transactionCoordinatorKey);
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(value.remaining());
        sb.append("@");
        sb.append(timestamp());
        return sb.toString();
    }

    @Override
    public boolean isLive()
    {
        return !isMarkedForDelete();
    }

    protected void validateName(CFMetaData metadata) throws MarshalException
    {
        AbstractType nameValidator = metadata.cfType == ColumnFamilyType.Super ? metadata.subcolumnComparator : metadata.comparator;
        nameValidator.validate(name());
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        AbstractType valueValidator = metadata.getValueValidator(name());
        if (valueValidator != null)
            valueValidator.validate(value());
    }

    @Override
    public boolean hasExpiredTombstones(int gcBefore)
    {
        return isMarkedForDelete() && getLocalDeletionTime() < gcBefore;
    }
}

