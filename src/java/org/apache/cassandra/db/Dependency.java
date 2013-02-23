package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.thrift.Dep;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.VersionUtil;

public class Dependency
{
    private static DependencySerializer serializer_ = new DependencySerializer();

    public static DependencySerializer serializer()
    {
        return serializer_;
    }

    private final ByteBuffer locatorKey;
    private final long timestamp;

    public Dependency(ByteBuffer locatorKey, long timestamp)
    {
        this.locatorKey = locatorKey;
        this.timestamp = timestamp;
    }

    public Dependency(Dep dep)
    {
        this.locatorKey = dep.locator_key;
        this.timestamp = dep.timestamp;
    }

    public ByteBuffer getLocatorKey()
    {
        return locatorKey;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public static class DependencySerializer implements ISerializer<Dependency>
    {
        @Override
        public void serialize(Dependency dependency, DataOutput dos) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(dependency.locatorKey, dos);
            dos.writeLong(dependency.timestamp);
        }

        @Override
        public Dependency deserialize(DataInput dis) throws IOException
        {
            ByteBuffer locatorKey = ByteBufferUtil.readWithShortLength(dis);
            long timestamp = dis.readLong();
            return new Dependency(locatorKey, timestamp);
        }

        @Override
        public long serializedSize(Dependency dependency)
        {
            int size = DBConstants.shortSize + dependency.locatorKey.remaining();
            size += DBConstants.longSize;
            return size;
        }
    }

    @Override
    public String toString()
    {
        return ByteBufferUtil.bytesToHex(locatorKey) + ":" + VersionUtil.toString(timestamp);
    }
}
