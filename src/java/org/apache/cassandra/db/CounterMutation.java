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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.transaction.PendingTransactionMutation;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HeapAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterMutation implements IMutation
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMutation.class);
    private static final CounterMutationSerializer serializer = new CounterMutationSerializer();

    private final RowMutation rowMutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(RowMutation rowMutation, ConsistencyLevel consistency)
    {
        this.rowMutation = rowMutation;
        this.consistency = consistency;
    }

    @Override
    public String getTable()
    {
        return rowMutation.getTable();
    }

    @Override
    public Collection<Integer> getColumnFamilyIds()
    {
        return rowMutation.getColumnFamilyIds();
    }

    @Override
    public Set<Dependency> getDependencies()
    {
        return rowMutation.getDependencies();
    }

    public Long extractTimestamp()
    {
        return rowMutation.extractTimestamp();
    }

    @Override
    public ByteBuffer key()
    {
        return rowMutation.key();
    }

    public RowMutation rowMutation()
    {
        return rowMutation;
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    public static CounterMutationSerializer serializer()
    {
        return serializer;
    }

    public RowMutation makeReplicationMutation() throws IOException
    {
        List<ReadCommand> readCommands = new LinkedList<ReadCommand>();
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            if (!columnFamily.metadata().getReplicateOnWrite())
                continue;
            addReadCommandFromColumnFamily(rowMutation.getTable(), rowMutation.key(), columnFamily, readCommands);
        }

        // create a replication RowMutation
        RowMutation replicationMutation = new RowMutation(rowMutation.getTable(), rowMutation.key());
        for (ReadCommand readCommand : readCommands)
        {
            Table table = Table.open(readCommand.table);
            Row row = readCommand.getRow(table);
            if (row == null || row.cf == null)
                continue;

            ColumnFamily cf = row.cf;
            if (cf.isSuper())
                cf.retainAll(rowMutation.getColumnFamily(cf.metadata().cfId));
            replicationMutation.add(cf);
        }
        return replicationMutation;
    }

    private void addReadCommandFromColumnFamily(String table, ByteBuffer key, ColumnFamily columnFamily, List<ReadCommand> commands)
    {
        QueryPath queryPath = new QueryPath(columnFamily.metadata().cfName);
        commands.add(new SliceByNamesReadCommand(table, key, queryPath, columnFamily.getColumnNames()));
    }

    public Message makeMutationMessage(int version) throws IOException
    {
        byte[] bytes = FBUtilities.serialize(this, serializer, version);
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.COUNTER_MUTATION, bytes, version);
    }

    public boolean shouldReplicateOnWrite()
    {
        for (ColumnFamily cf : rowMutation.getColumnFamilies())
            if (cf.metadata().getReplicateOnWrite())
                return true;
        return false;
    }

    @Override
    public void apply() throws IOException
    {
        // transform all CounterUpdateColumn to CounterColumn: accomplished by localCopy
        RowMutation rm = new RowMutation(rowMutation.getTable(), ByteBufferUtil.clone(rowMutation.key()));
        Table table = Table.open(rm.getTable());

        for (ColumnFamily cf_ : rowMutation.getColumnFamilies())
        {
            ColumnFamily cf = cf_.cloneMeShallow();
            ColumnFamilyStore cfs = table.getColumnFamilyStore(cf.id());
            for (IColumn column : cf_)
            {
                cf.addColumn(column.localCopy(cfs), HeapAllocator.instance);
            }
            rm.add(cf);
        }
        rm.apply();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    @Override
    public String toString(boolean shallow)
    {
        StringBuilder buff = new StringBuilder("CounterMutation(");
        buff.append(rowMutation.toString(shallow));
        buff.append(", ").append(consistency.toString());
        return buff.append(")").toString();
    }

    @Override
    public PendingTransactionMutation convertToPending(long pendingTime, long transactionId)
    {
        assert false : "To be implemented";
        return null;
    }

    @Override
    public void updateTimestamps(long timestamp, long localCommitTime)
    {
        assert false : "To be implemented";
    }

    public static class CounterMutationSerializer implements IVersionedSerializer<CounterMutation>
    {
        @Override
        public void serialize(CounterMutation cm, DataOutput dos, int version) throws IOException
        {
            RowMutation.serializer().serialize(cm.rowMutation(), dos, version);
            dos.writeUTF(cm.consistency().name());
        }

        @Override
        public CounterMutation deserialize(DataInput dis, int version) throws IOException
        {
            RowMutation rm = RowMutation.serializer().deserialize(dis, version);
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, dis.readUTF());
            return new CounterMutation(rm, consistency);
        }

        @Override
        public long serializedSize(CounterMutation cm, int version)
        {
            return RowMutation.serializer().serializedSize(cm.rowMutation(), version)
                   + DBConstants.shortSize + FBUtilities.encodedUTF8Length(cm.consistency().name());
        }
    }
}
