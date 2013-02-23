package org.apache.cassandra.utils;

import java.util.Collection;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnOrSuperColumnHelper {
    private static final Logger logger = LoggerFactory.getLogger(ColumnOrSuperColumnHelper.class);

    private ColumnOrSuperColumnHelper()
    {
        //don't instantiate this class
    }

    public static void updateTimestamp(ColumnOrSuperColumn cosc, long timestamp)
    {
        if (cosc.isSetColumn()) {
            cosc.column.timestamp = timestamp;
        } else if (cosc.isSetCounter_column()) {
            assert false : "Counter columns don't have timestamps";
        } else if (cosc.isSetSuper_column()) {
            for (Column column : cosc.super_column.columns) {
                column.timestamp = timestamp;
            }
        } else {
            assert cosc.isSetCounter_super_column();
            for (CounterColumn counter_column : cosc.counter_super_column.columns) {
                assert false : "Counter columns don't have timestamps";
            }
        }
    }

    public static class EvtAndLvt{
        private final long earliestValidTime;
        private final long latestValidTime;

        public EvtAndLvt(long earliestValidTime, long latestValidTime)
        {
            this.earliestValidTime = earliestValidTime;
            this.latestValidTime = latestValidTime;
        }

        public long getEarliestValidTime()
        {
            return earliestValidTime;
        }

        public long getLatestValidTime()
        {
            return latestValidTime;
        }

        @Override
        public String toString()
        {
            return earliestValidTime + "," + latestValidTime;
        }
    }

    public static EvtAndLvt extractEvtAndLvt(ColumnOrSuperColumn cosc)
    {
        EvtAndLvt result = null;
        if (cosc.isSetColumn()) {
            result = new EvtAndLvt(cosc.column.earliest_valid_time, cosc.column.latest_valid_time);
        } else if (cosc.isSetCounter_column()) {
            result = new EvtAndLvt(cosc.counter_column.earliest_valid_time, cosc.counter_column.latest_valid_time);
        } else if (cosc.isSetSuper_column()) {
            long maxEarliestValidTime = Long.MIN_VALUE;
            long minLatestValidTime = Long.MAX_VALUE;
            for (Column column : cosc.super_column.columns) {
                maxEarliestValidTime = Math.max(maxEarliestValidTime, column.earliest_valid_time);
                minLatestValidTime = Math.min(minLatestValidTime, column.latest_valid_time);
            }
            result = new EvtAndLvt(maxEarliestValidTime, minLatestValidTime);
        } else {
            long maxEarliestValidTime = Long.MIN_VALUE;
            long minLatestValidTime = Long.MAX_VALUE;
            for (CounterColumn column : cosc.counter_super_column.columns) {
                maxEarliestValidTime = Math.max(maxEarliestValidTime, column.earliest_valid_time);
                minLatestValidTime = Math.min(minLatestValidTime, column.latest_valid_time);
            }
            result = new EvtAndLvt(maxEarliestValidTime, minLatestValidTime);
        }

        logger.trace("extractEVTandLVT(" + toString(cosc) + ")=" + result);
        return result;
    }

    public static EvtAndLvt extractEvtAndLvt(Collection<ColumnOrSuperColumn> coscCollection)
    {
        long maxEarliestValidTime = Long.MIN_VALUE;
        long minLatestValidTime = Long.MAX_VALUE;
        for (ColumnOrSuperColumn cosc : coscCollection) {
            EvtAndLvt evtAndLvt = ColumnOrSuperColumnHelper.extractEvtAndLvt(cosc);
            maxEarliestValidTime = Math.max(maxEarliestValidTime, evtAndLvt.getEarliestValidTime());
            minLatestValidTime = Math.min(minLatestValidTime, evtAndLvt.getLatestValidTime());
        }
        return new EvtAndLvt(maxEarliestValidTime, minLatestValidTime);
    }

    public static long findLength(ColumnOrSuperColumn cosc)
    {
        if (cosc.isSetColumn()) {
            return cosc.column.value.remaining();
        } else if (cosc.isSetCounter_column()) {
            return DBConstants.longSize;
        } else if (cosc.isSetSuper_column()) {
            long length = 0;
            for (Column column : cosc.super_column.columns) {
                length += column.value.remaining();
            }
            return length;
        } else {
            assert cosc.isSetCounter_super_column();
            return DBConstants.longSize * cosc.counter_super_column.columns.size();
        }
    }

    public static String toString(ColumnOrSuperColumn cosc)
    {
        if (cosc.isSetColumn()) {
            StringBuilder sb = new StringBuilder(ByteBufferUtil.bytesToHex(cosc.column.name));
            sb.append(":");
            sb.append(cosc.column.isSetValue() ? ByteBufferUtil.bytesToHex(cosc.column.value) : "-");
            sb.append("@");
            sb.append(cosc.column.isSetTimestamp() ? cosc.column.timestamp : "-");
            return sb.toString();
        } else if (cosc.isSetCounter_column()) {
            return ByteBufferUtil.bytesToHex(cosc.counter_column.name) + ":" + cosc.counter_column.value;
        } else if (cosc.isSetSuper_column()) {
            String superColumnString = new String(ByteBufferUtil.bytesToHex(cosc.super_column.name) + "={");
            for (Column column : cosc.super_column.columns) {
                superColumnString += ByteBufferUtil.bytesToHex(column.name) + ":" + ByteBufferUtil.bytesToHex(column.value) + "@" + column.timestamp + ", ";
            }
            return superColumnString.substring(0, superColumnString.length() - 2) + "}";
        } else {
            String superColumnString = new String(ByteBufferUtil.bytesToHex(cosc.super_column.name) + "={");
            for (CounterColumn column : cosc.counter_super_column.columns) {
                superColumnString += ByteBufferUtil.bytesToHex(column.name) + ":" + column.value + ", ";
            }
            return superColumnString.substring(0, superColumnString.length() - 2) + "}";
        }
    }
}
