package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class DependencyCheck implements MessageProducer
{
    private static DependencyCheckSerializer serializer_ = new DependencyCheckSerializer();

    public static DependencyCheckSerializer serializer()
    {
        return serializer_;
    }

    private final Dependency dependency;
    private final InetAddress inquiringNode;

    public DependencyCheck(Dependency dependency)
    {
        this.dependency = dependency;
        this.inquiringNode = DatabaseDescriptor.getListenAddress();
    }

    public DependencyCheck(Dependency dependency, InetAddress inquiringNode)
    {
        this.dependency = dependency;
        this.inquiringNode = inquiringNode;
    }

    public Dependency getDependency()
    {
        return dependency;
    }

    public InetAddress getInquiringNode()
    {
        return inquiringNode;
    }

    public static DependencyCheck fromBytes(byte[] raw, int version) throws IOException
    {
        return serializer_.deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }

    public static class DependencyCheckSerializer implements IVersionedSerializer<DependencyCheck>
    {
        @Override
        public void serialize(DependencyCheck depCheck, DataOutput dos, int version) throws IOException
        {
            Dependency.serializer().serialize(depCheck.getDependency(), dos);
            dos.writeInt(depCheck.getInquiringNode().getAddress().length);
            dos.write(depCheck.getInquiringNode().getAddress());
        }

        @Override
        public DependencyCheck deserialize(DataInput dis, int version) throws IOException
        {
            Dependency dependency = Dependency.serializer().deserialize(dis);
            int addrSize = dis.readInt();
            byte[] rawAddr = new byte[addrSize];
            dis.readFully(rawAddr);
            InetAddress addr = InetAddress.getByAddress(rawAddr);
            return new DependencyCheck(dependency, addr);
        }

        @Override
        public long serializedSize(DependencyCheck depCheck, int version)
        {
            long size = Dependency.serializer().serializedSize(depCheck.getDependency());
            size += DBConstants.intSize;
            size += depCheck.getInquiringNode().getAddress().length;
            return size;
        }
    }

    @Override
    public Message getMessage(Integer version) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer_.serialize(this, dob, version);

        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.DEPENDENCY_CHECK, Arrays.copyOf(dob.getData(), dob.getLength()), version);
    }
}
