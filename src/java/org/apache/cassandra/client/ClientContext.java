package org.apache.cassandra.client;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.*;

/**
 * The client context and dependency tracking is quite simple in COPS2 because
 * we only care about "nearest" dependencies.
 *
 * @author wlloyd
 *
 */
public class ClientContext {
    // TODO: We could track the full key (columnPath and row key) and then we
    // could ensure we only have one dependency per full key
    private final HashSet<Dep> deps = new HashSet<Dep>();

    public final static int NOT_YET_SUPPORTED = -1;

    public HashSet<Dep> getDeps() {
        return deps;
    }

    public void addDep(Dep dep) {
        deps.add(dep);
    }

    public void addDeps(Set<Dep> deps) {
        for (Dep dep : deps) {
            addDep(dep);
        }
    }

    public void addDep(int not_yet_supported) {
        //Place holder for parts I haven't figured out yet
        assert not_yet_supported == NOT_YET_SUPPORTED;
        assert false;
    }

    public void clearDeps() {
        deps.clear();
    }

    public void addDep(ByteBuffer key, ColumnOrSuperColumn cosc)
    throws NotFoundException
    {
        // Note on deleted values. If we get a NotFoundException here then
        // garbage collection has removed the tombstone (if one ever existed) so
        // we don't need to add a dep on it.

        // If we get a value with the deleted time set, then we do need to add a
        // dep on that delete, and then (if required) throw a NotFoundException

        // We use the transactionCoordinatorKey instead of the supplied key if there is one in the returned column

        int fieldsSet = 0;
        if (cosc.isSetColumn()) {
            assert fieldsSet == 0;
            fieldsSet++;
            if (cosc.column.deleted_time != 0L) {
                assert cosc.column.value == null : "value should not be set if column was deleted";
                ByteBuffer depKey = cosc.column.isSetTransactionCoordinatorKey() ? cosc.column.transactionCoordinatorKey : key;
                addDep(new Dep(depKey, cosc.column.deleted_time));
                //Throw a NotFoundException because there isn't a column to return
                throw new NotFoundException();
            } else {
                ByteBuffer depKey = cosc.column.isSetTransactionCoordinatorKey() ? cosc.column.transactionCoordinatorKey : key;
                addDep(new Dep(depKey, cosc.column.timestamp));
            }
        }
        if (cosc.isSetSuper_column()) {
            assert fieldsSet == 0;
            fieldsSet++;
            boolean allColumnsDeleted = true;

            //check for super column being deleted
            if (cosc.super_column.deleted_time != 0L) {
                addDep(new Dep(key, cosc.super_column.deleted_time));
                throw new NotFoundException();
            }

            //go through each column in the super column, being careful of deleted columns
            for (Column column : cosc.super_column.columns) {
                if (column.deleted_time != 0L) {
                    assert column.value == null : "value should not be set if column was deleted";
                    ByteBuffer depKey = column.isSetTransactionCoordinatorKey() ? column.transactionCoordinatorKey : key;
                    addDep(new Dep(depKey, column.deleted_time));
                } else {
                    allColumnsDeleted = false;
                    ByteBuffer depKey = column.isSetTransactionCoordinatorKey() ? column.transactionCoordinatorKey : key;
                    addDep(new Dep(depKey, column.timestamp));
                }
            }
            if (allColumnsDeleted) {
                //Throw a NotFoundException because there are 0 columns to return
                throw new NotFoundException();
            }
        }
        if (cosc.isSetCounter_column()) {
            assert fieldsSet == 0;
            fieldsSet++;
            if (cosc.counter_column.deleted_time != 0L) {
                assert cosc.counter_column.value == 0L : "value should not be set if column was deleted";
                addDep(new Dep(key, cosc.counter_column.deleted_time));
                //Throw a NotFoundException because there are 0 columns to return to the client
                throw new NotFoundException();
            } else {
                for (Entry<Long, ByteBuffer> entry : cosc.counter_column.timestampToCoordinatorKey.entrySet()) {
                    Long timestamp = entry.getKey();
                    ByteBuffer coordinatorKey = entry.getValue();

                    if (coordinatorKey.remaining() == 0) {
                        //last add was not part of a transaction, use normal key for dep
                        addDep(new Dep(key, timestamp));
                    } else {
                        //last add was part of a transaction, use coordinator key for dep
                        addDep(new Dep(coordinatorKey, timestamp));
                    }
                }
            }
        }
        if (cosc.isSetCounter_super_column()) {
            assert fieldsSet == 0;
            fieldsSet++;

            boolean allColumnsDeleted = true;
            for (CounterColumn counter_column : cosc.counter_super_column.columns) {
                if (counter_column.deleted_time != 0L) {
                    assert counter_column.value == 0L : "value should not be set if column was deleted";
                    addDep(new Dep(key, counter_column.deleted_time));
                } else {
                    allColumnsDeleted = false;
                    for (Entry<Long, ByteBuffer> entry : cosc.counter_column.timestampToCoordinatorKey.entrySet()) {
                        Long timestamp = entry.getKey();
                        ByteBuffer coordinatorKey = entry.getValue();

                        if (coordinatorKey.remaining() == 0) {
                            //last add was not part of a transaction, use normal key for dep
                            addDep(new Dep(key, timestamp));
                        } else {
                            //last add was part of a transaction, use coordinator key for dep
                            addDep(new Dep(coordinatorKey, timestamp));
                        }
                    }
                }
            }
            if (allColumnsDeleted) {
                //Throw a NotFoundException because there are 0 columns to return
                throw new NotFoundException();
            }
        }
        assert fieldsSet == 1;

        return;
    }



    @Override
    public String toString() {
        return deps.toString();
    }
}
