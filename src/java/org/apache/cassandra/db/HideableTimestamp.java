package org.apache.cassandra.db;

import org.apache.cassandra.utils.VersionUtil;

public class HideableTimestamp
{
    private final long value;
    private final boolean visible;

    public HideableTimestamp(long value, boolean visible)
    {
        this.value = value;
        this.visible = visible;
    }

    public long getValue()
    {
        return value;
    }

    public boolean isVisible()
    {
        return visible;
    }

    @Override
    public String toString()
    {
        return VersionUtil.toString(value) + (visible ? "(v)" : "(h)");
    }
}