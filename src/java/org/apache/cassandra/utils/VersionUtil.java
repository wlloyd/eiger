package org.apache.cassandra.utils;

public class VersionUtil
{
    private VersionUtil()
    {
        //just helper methods, don't instantiate
    }

    public static short extractShortNodeId(long version)
    {
        return (short) (version & 0xFFFF);
    }

    public static long extractLamportTime(long version)
    {
        return version >>> 16;
    }

    public static String toString(long version)
    {
        return version + "=" + extractLamportTime(version) + "." + extractDatacenter(version) + "." + (extractShortNodeId(version) & 0x0FFF);
    }

    public static byte extractDatacenter(long version)
    {
        return (byte) ((version & 0xF000) >>> 12);
    }
}
