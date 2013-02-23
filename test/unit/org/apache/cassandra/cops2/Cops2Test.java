package org.apache.cassandra.cops2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.client.ClientLibrary.CopsTestingConcurrentWriteHook;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.LamportClock;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

public class Cops2Test extends CleanupHelper
{
    private final static ByteBuffer BOUND = ByteBufferUtil.EMPTY_BYTE_BUFFER;
    private final static SliceRange FULL_SLICE_RANGE = new SliceRange().setStart(BOUND).setFinish(BOUND).setCount(1000);
    private final static SlicePredicate FULL_SLICE_PREDICATE = new SlicePredicate().setSlice_range(FULL_SLICE_RANGE);

    private static Map<String, Integer> localServerIPAndPorts;
    private static List<Map<String, Integer>> dcToServerIPAndPorts = null;
    private static ConsistencyLevel consistencyLevel;

    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException, InvalidRequestException, SchemaDisagreementException, TException
    {
        assert false : "Use subclasses to test";
    }

    public static void setLocalServerIPAndPorts(Map<String, Integer> localServerIPAndPorts2)
    {
        localServerIPAndPorts = localServerIPAndPorts2;
    }

    public static void setDcToServerIPAndPorts(List<Map<String, Integer>> dcToServerIPAndPorts2)
    {
        dcToServerIPAndPorts = dcToServerIPAndPorts2;
    }


    public static void setConsistencyLevel(ConsistencyLevel consistencyLevel2)
    {
        consistencyLevel = consistencyLevel2;
    }

    public ByteBuffer randomKey()
    {
        if (this instanceof EmbeddedCops2Test) {
            //EmbeddedCops2Test requires UTF-8 encoded keys, so we'll generate them here the same way OrderPreservingPartitioner.getRandomToken() generates them
            String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Random r = new Random();
            StringBuilder buffer = new StringBuilder();
            for (int j = 0; j < 16; j++) {
                buffer.append(chars.charAt(r.nextInt(chars.length())));
            }
            return ByteBufferUtil.bytes(buffer.toString());

        } else {
            //using randomUUID as suggested by: http://wiki.apache.org/cassandra/ByteOrderedPartitioner
            return ByteBuffer.wrap(UUIDGen.decompose(UUID.randomUUID()));
        }
    }

    private static Column newColumn(String name) {
        return new Column(ByteBufferUtil.bytes(name));
    }

    private static Column newColumn(String name, String value) {
        return new Column(ByteBufferUtil.bytes(name)).setValue(ByteBufferUtil.bytes(value)).setTimestamp(0L);
    }

    private static Column newColumn(String name, String value, long timestamp) {
        return new Column(ByteBufferUtil.bytes(name)).setValue(ByteBufferUtil.bytes(value)).setTimestamp(timestamp);
    }

    private static CounterColumn newCounterColumn(String name, long value) {
        return new CounterColumn(ByteBufferUtil.bytes(name), value);
    }


    private static ColumnParent newColumnParent(String columnFamily, String superColumn) {
        return new ColumnParent(columnFamily).setSuper_column(ByteBufferUtil.bytes(superColumn));
    }


    //fields that are null will not be set
    private static ColumnPath newColumnPath(String columnFamily, String superColumn, String column) {
        ColumnPath columnPath = new ColumnPath(columnFamily);
        if (superColumn != null) {
            columnPath.setSuper_column(ByteBufferUtil.bytes(superColumn));
        }
        if (column != null) {
            columnPath.setColumn(ByteBufferUtil.bytes(column));
        }
        return columnPath;
    }

    private static SuperColumn newSuperColumn(String name, List<Column> columns) {
            return new SuperColumn().setName(ByteBufferUtil.bytes(name)).setColumns(columns);
    }

    private static Mutation newMutation(Column column) {
        return new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
    }

    private static Mutation newMutation(SuperColumn super_column) {
        return new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setSuper_column(super_column));
    }

    private HashMap<ByteBuffer,Map<String,List<Mutation>>> newMutationMap(List<ByteBuffer> keys, List<String> columnFamilies, List<Mutation> mutations)
    {
        assert keys.size() == columnFamilies.size();
        assert keys.size() == mutations.size();

        HashMap<ByteBuffer,Map<String,List<Mutation>>> mutationMap = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();

        for (int i = 0; i < keys.size(); i++) {
            ByteBuffer key = keys.get(i);
            String columnFamily = columnFamilies.get(i);
            Mutation mutation = mutations.get(i);

            ArrayList<Mutation> mutationList = new ArrayList<Mutation>();
            mutationList.add(mutation);

            HashMap<String,List<Mutation>> innerMutationMap = new HashMap<String,List<Mutation>>();
            innerMutationMap.put(columnFamily, mutationList);

            mutationMap.put(key, innerMutationMap);
        }

        return mutationMap;
    }

    private HashMap<ByteBuffer,Map<String,List<Mutation>>> newMutationMap(ByteBuffer key, String columnFamily, Mutation mutation)
    {
        ArrayList<Mutation> mutationList = new ArrayList<Mutation>();
        mutationList.add(mutation);

        HashMap<String,List<Mutation>> innerMutationMap = new HashMap<String,List<Mutation>>();
        innerMutationMap.put(columnFamily, mutationList);

        HashMap<ByteBuffer,Map<String,List<Mutation>>> mutationMap = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();
        mutationMap.put(key, innerMutationMap);

        return mutationMap;
    }

    @Test
    public void testEmbeddedCassandraService()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id = ByteBufferUtil.bytes("1");

        long timestamp = System.currentTimeMillis();
        ColumnPath cp = new ColumnPath("Standard1");
        ColumnParent par = new ColumnParent("Standard1");
        cp.column = ByteBufferUtil.bytes("name");

        // insert
        clientLibrary.insert(key_user_id, par, new Column(ByteBufferUtil.bytes("name")).setValue(ByteBufferUtil.bytes("Ran")).setTimestamp(timestamp));

        // read
        ColumnOrSuperColumn got = clientLibrary.get(key_user_id, cp);

        // assert
        assertNotNull("Got a null ColumnOrSuperColumn", got);
        assertEquals("Ran", ByteBufferUtil.string(got.getColumn().value));

        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testEmbeddedCassandraService passed");
    }

    @Test
    public void testInsertGet()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id = randomKey();

        long timestamp = System.currentTimeMillis();
        ColumnParent par = new ColumnParent("Standard1");
        clientLibrary.insert(key_user_id, par, newColumn("col1", "val1", timestamp));
        clientLibrary.insert(key_user_id, par, newColumn("col2", "val2", timestamp));
        clientLibrary.insert(key_user_id, par, newColumn("col3", "val3", timestamp));


        ColumnPath cp = new ColumnPath("Standard1");
        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, cp);
        cp.column = ByteBufferUtil.bytes("col2");
        ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, cp);
        cp.column = ByteBufferUtil.bytes("col3");
        ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id, cp);

        assertNotNull("Got a null ColumnOrSuperColumn", got1);
        assertNotNull("Got a null ColumnOrSuperColumn", got2);
        assertNotNull("Got a null ColumnOrSuperColumn", got3);
        assertEquals("val1", ByteBufferUtil.string(got1.getColumn().value));
        assertEquals("val2", ByteBufferUtil.string(got2.getColumn().value));
        assertEquals("val3", ByteBufferUtil.string(got3.getColumn().value));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testInsertGet passed");
    }

    @Test
    public void testInsertGetDeps()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id = randomKey();

        long timestamp = System.currentTimeMillis();
        ColumnParent par = new ColumnParent("Standard1");

        clientLibrary.insert(key_user_id, par, newColumn("col1", "val1", timestamp));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.insert(key_user_id, par, newColumn("col2", "val2", timestamp));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.insert(key_user_id, par, newColumn("col3", "val3", timestamp));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();


        ColumnPath cp = new ColumnPath("Standard1");
        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, cp);
        assertEquals(2, clientLibrary.getContext().getDeps().size());
        HashSet<Dep> test_deps = new HashSet<Dep>();
        test_deps.add(dep3);
        test_deps.add(dep1);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        cp.column = ByteBufferUtil.bytes("col2");
        ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, cp);
        test_deps.add(dep2);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        cp.column = ByteBufferUtil.bytes("col3");
        ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id, cp);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        //duplicate tests from testInsertGet
        assertNotNull("Got a null ColumnOrSuperColumn", got1);
        assertNotNull("Got a null ColumnOrSuperColumn", got2);
        assertNotNull("Got a null ColumnOrSuperColumn", got3);
        assertEquals("val1", ByteBufferUtil.string(got1.getColumn().value));
        assertEquals("val2", ByteBufferUtil.string(got2.getColumn().value));
        assertEquals("val3", ByteBufferUtil.string(got3.getColumn().value));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testInsertGetDeps passed");
    }


    @Test
    public void testInsertGetDeps2()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id1 = randomKey();
        ByteBuffer key_user_id2 = randomKey();
        ByteBuffer key_user_id3 = randomKey();

        ColumnParent par1 = new ColumnParent("Standard1");
        ColumnParent par2 = new ColumnParent("Standard2");
        ColumnParent par3 = new ColumnParent("Standard3");


        clientLibrary.insert(key_user_id1, par1, newColumn("col1", "val1", 0L));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.insert(key_user_id2, par2, newColumn("col2", "val2", 0L));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.insert(key_user_id3, par3, newColumn("col3", "val3", 0L));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();


        ColumnPath cp = new ColumnPath("Standard1");
        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id1, cp);
        assertEquals(2, clientLibrary.getContext().getDeps().size());
        HashSet<Dep> test_deps = new HashSet<Dep>();
        test_deps.add(dep3);
        test_deps.add(dep1);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        cp = new ColumnPath("Standard2");
        cp.column = ByteBufferUtil.bytes("col2");
        ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id2, cp);
        test_deps.add(dep2);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        cp = new ColumnPath("Standard3");
        cp.column = ByteBufferUtil.bytes("col3");
        ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id3, cp);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        //duplicate tests from testInsertGet
        assertNotNull("Got a null ColumnOrSuperColumn", got1);
        assertNotNull("Got a null ColumnOrSuperColumn", got2);
        assertNotNull("Got a null ColumnOrSuperColumn", got3);
        assertEquals("val1", ByteBufferUtil.string(got1.getColumn().value));
        assertEquals("val2", ByteBufferUtil.string(got2.getColumn().value));
        assertEquals("val3", ByteBufferUtil.string(got3.getColumn().value));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testInsertGetDeps2 passed");
    }

    @Test
    public void testInsertRemoveGet()
    throws Exception
    {
        // 5 test cases
        //Case 1: ColumnFamily/SuperColumn/Column, remove full path
        // Subcase a: get full path
        // Subcase b: get SuperColumn
        //Case 2: ColumnFamily/SuperColumn/Column, remove CF/SC path
        // Subcase a: get full path
        // Subcase b: get SuperColumn
        //Case 3: ColumnFamily/SuperColumn/Column, remove CF path
        // Subcase a: get full path
        // Subcase b: get SuperColumn
        //Case 4: ColumnFamily/Column, remove full path
        //Case 5: ColumnFamily/Column, remove CF path


        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key = randomKey();

        //Note: Prefix of ColumnFamily name defines if it's a standard or a super columnfamily
        ColumnParent triple_parent = newColumnParent("Super1", "SuperCol1");
        ColumnPath triple_full_path = newColumnPath("Super1", "SuperCol1", "column01");
        ColumnPath triple_partial_path = newColumnPath("Super1", "SuperCol1", null);
        ColumnPath triple_cf_only_path = new ColumnPath("Super1");

        ColumnParent double_parent = new ColumnParent("Standard1");
        ColumnPath double_full_path = newColumnPath("Standard1", null, "column01");
        ColumnPath double_cf_only_path = new ColumnPath("Standard1");

        //Note: It seems that in a superColumn, the column names must be 8 bytes long
        Column column = newColumn("column01", "val1", 0L);


        //Case 1: ColumnFamily/SuperColumn/Column, remove full path
        clientLibrary.insert(key, triple_parent, column);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        //Remove fully specified columnPath
        clientLibrary.remove(key, triple_full_path);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1removed = clientLibrary.getContext().getDeps().iterator().next();

        //Subcase a: get fully specified
        //Now we clear deps to ensure we get a dep on the remove
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got1a = clientLibrary.get(key, triple_full_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1removed, clientLibrary.getContext().getDeps().iterator().next());

        //Subcase b: get SuperColumn
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got1b = clientLibrary.get(key, triple_partial_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1removed, clientLibrary.getContext().getDeps().iterator().next());


        //Case 2: ColumnFamily/SuperColumn/Column, remove CF/SC path
        clientLibrary.insert(key, triple_parent, column);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        //Remove partially specified columnPath
        clientLibrary.remove(key, triple_partial_path);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2removed = clientLibrary.getContext().getDeps().iterator().next();

        //Subcase a: get full specified
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got2a = clientLibrary.get(key, triple_full_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2removed, clientLibrary.getContext().getDeps().iterator().next());

        //Subcase b: get SC specified only
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got2b = clientLibrary.get(key, triple_partial_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2removed, clientLibrary.getContext().getDeps().iterator().next());


        //Case 3: ColumnFamily/SuperColumn/Column, remove CF path
        clientLibrary.insert(key, triple_parent, column);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        //Remove CF path only
        clientLibrary.remove(key, triple_cf_only_path);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3removed = clientLibrary.getContext().getDeps().iterator().next();

        //Subcase a: get full specified
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got3a = clientLibrary.get(key, triple_full_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3removed, clientLibrary.getContext().getDeps().iterator().next());

        //Subcase b: get SC specified only
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got3b = clientLibrary.get(key, triple_partial_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3removed, clientLibrary.getContext().getDeps().iterator().next());


        //Case 4: ColumnFamily/Column, remove full path
        clientLibrary.insert(key, double_parent, column);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep4 = clientLibrary.getContext().getDeps().iterator().next();

        //Remove fully specified columnPath
        clientLibrary.remove(key, double_full_path);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep4removed = clientLibrary.getContext().getDeps().iterator().next();

        //Now we clear deps to ensure we get a dep on the remove
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got4 = clientLibrary.get(key, double_full_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep4removed, clientLibrary.getContext().getDeps().iterator().next());


        //Case 5: ColumnFamily/Column, remove CF path
        clientLibrary.insert(key, double_parent, column);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5 = clientLibrary.getContext().getDeps().iterator().next();

        //Remove fully specified columnPath
        clientLibrary.remove(key, double_cf_only_path);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5removed = clientLibrary.getContext().getDeps().iterator().next();

        //Now we clear deps to ensure we get a dep on the remove
        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got5 = clientLibrary.get(key, double_full_path);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep5removed, clientLibrary.getContext().getDeps().iterator().next());


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());
        clientLibrary.getAnyClient().truncate("Super1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testInsertRemoveGet passed");
    }

    @Test
    public void testBatchMutateOfOne()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = randomKey();

        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
        ColumnPath super1_supercol1_column02 = newColumnPath("Super1", "SuperCol1", "column02");
        ColumnPath super1_supercol2 = newColumnPath("Super1", "SuperCol2", null);
        ArrayList<Column> oneColumnList = new ArrayList<Column>();
        ArrayList<ByteBuffer> oneColumnNameList = new ArrayList<ByteBuffer>();
        //column names must be 8 bytes in a super column
        oneColumnList.add(newColumn("column02", "value2"));
        oneColumnNameList.add(oneColumnList.get(0).name);

        ArrayList<Column> twoColumnList = new ArrayList<Column>();
        ArrayList<ByteBuffer> twoColumnNameList = new ArrayList<ByteBuffer>();
        //column names must be 8 bytes in a super column
        twoColumnList.add(newColumn("column03", "value3"));
        twoColumnList.add(newColumn("column04", "value4"));
        twoColumnNameList.add(twoColumnList.get(0).name);
        twoColumnNameList.add(twoColumnList.get(1).name);

        //Relatively exhaustive set of mutations.
        Mutation insertColumnMutation = newMutation(newColumn("col1", "value1"));
        Mutation insertSuperColumnOfOneMutation = newMutation(newSuperColumn("SuperCol1", oneColumnList));
        Mutation insertSuperColumnOfTwoMutation = newMutation(newSuperColumn("SuperCol2", twoColumnList));


        Mutation deleteColumnFamilyMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L));
        Mutation deleteSuperColumnOfOneMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol1")));
        Mutation deleteSuperColumnOfTwoMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol2")));

        Mutation deleteSlicePredicateOfOneNameMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol1")).setPredicate(new SlicePredicate().setColumn_names(oneColumnNameList)));
        Mutation deleteSlicePredicateOfTwoNamesMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol2")).setPredicate(new SlicePredicate().setColumn_names(twoColumnNameList)));
        //Cassandra does not yet support deletions of slice ranges, so we can't test it -- that would also make things quite tricky!
        //Mutation deleteFullSliceRange = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setPredicate(new SlicePredicate().setSlice_range(FULL_SLICE_RANGE)));


        //Test each type of mutation

        // Insert Column
        clientLibrary.batch_mutate(newMutationMap(key1, "Standard1", insertColumnMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        ColumnOrSuperColumn got1 = clientLibrary.get(key1, standard1_col1);
        assertEquals("value1", ByteBufferUtil.string(got1.column.value));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());

        // Delete whole Column Family
        clientLibrary.batch_mutate(newMutationMap(key1, "Standard1", deleteColumnFamilyMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1DeleteCF = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got1DeleteCF = clientLibrary.get(key1, standard1_col1);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1DeleteCF, clientLibrary.getContext().getDeps().iterator().next());

        //Insert super column with one column
        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfOneMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        ColumnOrSuperColumn got2 = clientLibrary.get(key1, super1_supercol1_column02);
        assertEquals("value2", ByteBufferUtil.string(got2.column.value));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2, clientLibrary.getContext().getDeps().iterator().next());

        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", deleteSuperColumnOfOneMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2DeleteSC = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got2DeleteSC = clientLibrary.get(key1, super1_supercol1_column02);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2DeleteSC, clientLibrary.getContext().getDeps().iterator().next());

        // insert again, so we can try the other delete type
        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfOneMutation));
        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", deleteSlicePredicateOfOneNameMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2DeleteSP = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got2DeleteSP = clientLibrary.get(key1, super1_supercol1_column02);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2DeleteSP, clientLibrary.getContext().getDeps().iterator().next());


        //Insert super column with two columns
        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfTwoMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        ColumnOrSuperColumn got3 = clientLibrary.get(key1, super1_supercol2);
        assertTrue(got3.isSetSuper_column());
        assertEquals(2, got3.super_column.columns.size());
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3, clientLibrary.getContext().getDeps().iterator().next());

        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", deleteSuperColumnOfTwoMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3DeleteSC = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got3DeleteSP = clientLibrary.get(key1, super1_supercol2);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3DeleteSC, clientLibrary.getContext().getDeps().iterator().next());

        // insert again, so we can try the other delete type
        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfTwoMutation));
        clientLibrary.batch_mutate(newMutationMap(key1, "Super1", deleteSlicePredicateOfTwoNamesMutation));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3DeleteSP = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got3DeleteSP = clientLibrary.get(key1, super1_supercol2);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3DeleteSP, clientLibrary.getContext().getDeps().iterator().next());


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());
        clientLibrary.getAnyClient().truncate("Super1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testBatchMutateOfOne passed");
    }

    @Test
    public void testBatchMutateOfMany()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);


        ArrayList<ByteBuffer> threeKeys = new ArrayList<ByteBuffer>();
        ByteBuffer key1 = randomKey();
        ByteBuffer key2 = randomKey();
        ByteBuffer key3 = randomKey();
        threeKeys.add(key1);
        threeKeys.add(key2);
        threeKeys.add(key3);

        ArrayList<String> threeColumnFamilies = new ArrayList<String>();
        threeColumnFamilies.add("Standard1");
        threeColumnFamilies.add("Standard1");
        threeColumnFamilies.add("Standard1");

        ArrayList<Mutation> threeMutations = new ArrayList<Mutation>();
        Mutation insertColumnMutation1 = newMutation(newColumn("col1", "value1"));
        Mutation insertColumnMutation2 = newMutation(newColumn("col2", "value2"));
        Mutation insertColumnMutation3 = newMutation(newColumn("col3", "value3"));
        threeMutations.add(insertColumnMutation1);
        threeMutations.add(insertColumnMutation2);
        threeMutations.add(insertColumnMutation3);


        clientLibrary.batch_mutate(newMutationMap(threeKeys, threeColumnFamilies, threeMutations));
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        HashSet<Dep> threeDeps = clientLibrary.getContext().getDeps();

        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
        ColumnPath standard1_col2 = newColumnPath("Standard1", null, "col2");
        ColumnPath standard1_col3 = newColumnPath("Standard1", null, "col3");


        clientLibrary.getContext().clearDeps();

        clientLibrary.get(key1, standard1_col1);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertTrue(threeDeps.containsAll(clientLibrary.getContext().getDeps()));

        clientLibrary.get(key2, standard1_col2);
        assertEquals(2, clientLibrary.getContext().getDeps().size());
        assertTrue(threeDeps.containsAll(clientLibrary.getContext().getDeps()));

        clientLibrary.get(key3, standard1_col3);
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        assertTrue(threeDeps.containsAll(clientLibrary.getContext().getDeps()));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testBatchMutateOfMany passed");
    }

    @Test
    public void testGetSlice()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = randomKey();
        ColumnParent par = new ColumnParent("Standard1");

        clientLibrary.insert(key1, par, newColumn("col1", "val1"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col2", "val2"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col3", "val3"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();


        //test slice of 1
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfOne = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col1")));
        clientLibrary.get_slice(key1, par, sliceOfOne);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());

        //test slice of 3
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfThree = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col3")));
        clientLibrary.get_slice(key1, par, sliceOfThree);
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3));

        //test slice of 3 deleted down to 1 result
        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
        ColumnPath standard1_col3 = newColumnPath("Standard1", null, "col3");

        clientLibrary.remove(key1, standard1_col1);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key1, standard1_col3);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        List<ColumnOrSuperColumn> sliceResult = clientLibrary.get_slice(key1, par, sliceOfThree);
        assertEquals(1, sliceResult.size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3removed));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testGetSlice passed");
    }

    @Test
    public void testMultiGetSlice()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = randomKey();
        ByteBuffer key2 = randomKey();
        ByteBuffer key3 = randomKey();
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        ColumnParent par = new ColumnParent("Standard1");

        clientLibrary.insert(key1, par, newColumn("col1", "val1"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col2", "val2"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col3", "val3"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col1", "val4"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep4 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col2", "val5"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col1", "val6"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col2", "val7"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7 = clientLibrary.getContext().getDeps().iterator().next();


        //test slice of 1 for all 3 keys
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfOne = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col1")));

        Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetResults = clientLibrary.multiget_slice(keys, par, sliceOfOne);
        assertEquals(3, multigetResults.size());
        assertEquals(1, multigetResults.get(key1).size());
        assertEquals(1, multigetResults.get(key2).size());
        assertEquals(1, multigetResults.get(key3).size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));


        //test slice of everything
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfThree = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col3")));
        multigetResults = clientLibrary.multiget_slice(keys, par, sliceOfThree);
        assertEquals(3, multigetResults.size());
        assertEquals(3, multigetResults.get(key1).size());
        assertEquals(2, multigetResults.get(key2).size());
        assertEquals(2, multigetResults.get(key3).size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7));


        //test slice of 3 deleted down to 1 result, slice of 2 deleted down to 1 result, and slice of 2 deleted down to 0
        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
        ColumnPath standard1_col2 = newColumnPath("Standard1", null, "col2");
        ColumnPath standard1_col3 = newColumnPath("Standard1", null, "col3");

        clientLibrary.remove(key1, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key1, standard1_col3);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key2, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col1);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        multigetResults = clientLibrary.multiget_slice(keys, par, sliceOfThree);
        assertEquals(3, multigetResults.size());
        assertEquals(1, multigetResults.get(key1).size());
        assertEquals(1, multigetResults.get(key2).size());
        assertEquals(0, multigetResults.get(key3).size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7removed));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testMultiGetSlice passed");
    }

    @Test
    public void testGetCount()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = randomKey();
        ColumnParent par = new ColumnParent("Standard1");

        ArrayList<Dep> deps = new ArrayList<Dep>();
        for (int i = 0; i < 100; i++) {
            clientLibrary.insert(key1, par, newColumn("col" + i, "val" + i));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            deps.add(clientLibrary.getContext().getDeps().iterator().next());
        }


        clientLibrary.getContext().clearDeps();
        int count = clientLibrary.get_count(key1, par, FULL_SLICE_PREDICATE);
        assertEquals(100, count);
        assertEquals(100, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 100; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
        }

        //get 10 results from the middle of the range
        //Explicitly name the columns in this predicate instead of giving a slice
        ArrayList<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (int i = 50; i < 60; i++) {
            columnNames.add(ByteBufferUtil.bytes("col" + i));
        }
        SlicePredicate slice50s = new SlicePredicate().setColumn_names(columnNames);
        clientLibrary.getContext().clearDeps();
        count = clientLibrary.get_count(key1, par, slice50s);
        assertEquals(10, count);
        assertEquals(10, clientLibrary.getContext().getDeps().size());
        for (int i = 50; i < 59; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
        }


        //delete odd columns
        Dep[] removedDeps = new Dep[100];
        for (int i = 1; i < 100; i += 2) {
            ColumnPath columnPath = newColumnPath("Standard1", null, "col" + i);
            clientLibrary.remove(key1, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i] = clientLibrary.getContext().getDeps().iterator().next();
        }

        clientLibrary.getContext().clearDeps();
        count = clientLibrary.get_count(key1, par, FULL_SLICE_PREDICATE);
        assertEquals(50, count);
        assertEquals(100, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
            } else {
                assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
            }
        }

        //get 10 results from the middle of the range
        clientLibrary.getContext().clearDeps();
        count = clientLibrary.get_count(key1, par, slice50s);
        assertEquals(5, count);
        assertEquals(10, clientLibrary.getContext().getDeps().size());
        for (int i = 50; i < 59; i++) {
            if (i % 2 == 0) {
                assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
            } else {
                assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
            }
        }


        //delete even columns
        for (int i = 0; i < 100; i += 2) {
            ColumnPath columnPath = newColumnPath("Standard1", null, "col" + i);
            clientLibrary.remove(key1, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i] = clientLibrary.getContext().getDeps().iterator().next();
        }

        clientLibrary.getContext().clearDeps();
        count = clientLibrary.get_count(key1, par, FULL_SLICE_PREDICATE);
        assertEquals(0, count);
        assertEquals(100, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 100; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
        }

        //get 10 results from the middle of the range
        clientLibrary.getContext().clearDeps();
        count = clientLibrary.get_count(key1, par, slice50s);
        assertEquals(0, count);
        assertEquals(10, clientLibrary.getContext().getDeps().size());
        for (int i = 50; i < 59; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
        }


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testGetCount passed");
    }

    @Test
    public void testMultiGetCount()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = randomKey();
        ByteBuffer key2 = randomKey();
        ByteBuffer key3 = randomKey();
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        ColumnParent par = new ColumnParent("Standard1");

        ArrayList<Dep> deps = new ArrayList<Dep>();
        for (int i = 0; i < 100; i++) {
            clientLibrary.insert(key1, par, newColumn("col" + i, "val" + i));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            deps.add(clientLibrary.getContext().getDeps().iterator().next());

            clientLibrary.insert(key2, par, newColumn("col" + i, "val" + i));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            deps.add(clientLibrary.getContext().getDeps().iterator().next());

            clientLibrary.insert(key3, par, newColumn("col" + i, "val" + i));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            deps.add(clientLibrary.getContext().getDeps().iterator().next());
        }


        clientLibrary.getContext().clearDeps();
        Map<ByteBuffer, Integer> keyToCount = clientLibrary.multiget_count(keys, par, FULL_SLICE_PREDICATE);
        assertEquals(100, keyToCount.get(key1).intValue());
        assertEquals(100, keyToCount.get(key2).intValue());
        assertEquals(100, keyToCount.get(key3).intValue());
        assertEquals(300, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 300; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
        }

        //get 10 results from the middle of the range
        //Explicitly name the columns in this predicate instead of giving a slice
        ArrayList<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (int i = 50; i < 60; i++) {
            columnNames.add(ByteBufferUtil.bytes("col" + i));
        }
        SlicePredicate slice50s = new SlicePredicate().setColumn_names(columnNames);
        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.multiget_count(keys, par, slice50s);
        assertEquals(10, keyToCount.get(key1).intValue());
        assertEquals(10, keyToCount.get(key2).intValue());
        assertEquals(10, keyToCount.get(key3).intValue());
        assertEquals(30, clientLibrary.getContext().getDeps().size());
        for (int i = 50*3; i < 59*3; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
        }


        //delete odd columns
        Dep[] removedDeps = new Dep[300];
        for (int i = 1; i < 100; i += 2) {
            ColumnPath columnPath = newColumnPath("Standard1", null, "col" + i);
            clientLibrary.remove(key1, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key2, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+1] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key3, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+2] = clientLibrary.getContext().getDeps().iterator().next();
        }

        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.multiget_count(keys, par, FULL_SLICE_PREDICATE);
        assertEquals(50, keyToCount.get(key1).intValue());
        assertEquals(50, keyToCount.get(key2).intValue());
        assertEquals(50, keyToCount.get(key3).intValue());
        assertEquals(300, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 300; i++) {
            if ((i/3) % 2 == 0) {
                assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
            } else {
                assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
            }
        }

        //get 10 results from the middle of the range
        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.multiget_count(keys, par, slice50s);
        assertEquals(5, keyToCount.get(key1).intValue());
        assertEquals(5, keyToCount.get(key2).intValue());
        assertEquals(5, keyToCount.get(key3).intValue());
        assertEquals(30, clientLibrary.getContext().getDeps().size());
        for (int i = 50*3; i < 59*3; i++) {
            if ((i/3) % 2 == 0) {
                assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
            } else {
                assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
            }
        }


        //delete even columns
        for (int i = 0; i < 100; i += 2) {
            ColumnPath columnPath = newColumnPath("Standard1", null, "col" + i);
            clientLibrary.remove(key1, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key2, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+1] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key3, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+2] = clientLibrary.getContext().getDeps().iterator().next();
        }

        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.multiget_count(keys, par, FULL_SLICE_PREDICATE);
        assertEquals(0, keyToCount.get(key1).intValue());
        assertEquals(0, keyToCount.get(key2).intValue());
        assertEquals(0, keyToCount.get(key3).intValue());
        assertEquals(300, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 300; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
        }

        //get 10 results from the middle of the range
        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.multiget_count(keys, par, slice50s);
        assertEquals(0, keyToCount.get(key1).intValue());
        assertEquals(0, keyToCount.get(key2).intValue());
        assertEquals(0, keyToCount.get(key3).intValue());
        assertEquals(30, clientLibrary.getContext().getDeps().size());
        for (int i = 50*3; i < 59*3; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
        }


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testMultiGetCount passed");
    }

    //WL TODO: Robustly test transaction_transactional_multiget_count using concurrent writes
    @Test
    public void testTransactionalMultiGetCount()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = randomKey();
        ByteBuffer key2 = randomKey();
        ByteBuffer key3 = randomKey();
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        ColumnParent par = new ColumnParent("Standard1");

        ArrayList<Dep> deps = new ArrayList<Dep>();
        for (int i = 0; i < 100; i++) {
            clientLibrary.insert(key1, par, newColumn("col" + i, "val" + i));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            deps.add(clientLibrary.getContext().getDeps().iterator().next());

            clientLibrary.insert(key2, par, newColumn("col" + i, "val" + i));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            deps.add(clientLibrary.getContext().getDeps().iterator().next());

            clientLibrary.insert(key3, par, newColumn("col" + i, "val" + i));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            deps.add(clientLibrary.getContext().getDeps().iterator().next());
        }


        clientLibrary.getContext().clearDeps();
        Map<ByteBuffer, Integer> keyToCount = clientLibrary.transactional_multiget_count(keys, par, FULL_SLICE_PREDICATE);
        assertEquals(100, keyToCount.get(key1).intValue());
        assertEquals(100, keyToCount.get(key2).intValue());
        assertEquals(100, keyToCount.get(key3).intValue());
        assertEquals(300, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 300; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
        }

        //get 10 results from the middle of the range
        //Explicitly name the columns in this predicate instead of giving a slice
        ArrayList<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (int i = 50; i < 60; i++) {
            columnNames.add(ByteBufferUtil.bytes("col" + i));
        }
        SlicePredicate slice50s = new SlicePredicate().setColumn_names(columnNames);
        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.transactional_multiget_count(keys, par, slice50s);
        assertEquals(10, keyToCount.get(key1).intValue());
        assertEquals(10, keyToCount.get(key2).intValue());
        assertEquals(10, keyToCount.get(key3).intValue());
        assertEquals(30, clientLibrary.getContext().getDeps().size());
        for (int i = 50*3; i < 59*3; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
        }


        //delete odd columns
        Dep[] removedDeps = new Dep[300];
        for (int i = 1; i < 100; i += 2) {
            ColumnPath columnPath = newColumnPath("Standard1", null, "col" + i);
            clientLibrary.remove(key1, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key2, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+1] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key3, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+2] = clientLibrary.getContext().getDeps().iterator().next();
        }

        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.transactional_multiget_count(keys, par, FULL_SLICE_PREDICATE);
        assertEquals(50, keyToCount.get(key1).intValue());
        assertEquals(50, keyToCount.get(key2).intValue());
        assertEquals(50, keyToCount.get(key3).intValue());
        assertEquals(300, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 300; i++) {
            if ((i/3) % 2 == 0) {
                assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
            } else {
                assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
            }
        }

        //get 10 results from the middle of the range
        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.transactional_multiget_count(keys, par, slice50s);
        assertEquals(5, keyToCount.get(key1).intValue());
        assertEquals(5, keyToCount.get(key2).intValue());
        assertEquals(5, keyToCount.get(key3).intValue());
        assertEquals(30, clientLibrary.getContext().getDeps().size());
        for (int i = 50*3; i < 59*3; i++) {
            if ((i/3) % 2 == 0) {
                assertTrue(clientLibrary.getContext().getDeps().contains(deps.get(i)));
            } else {
                assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
            }
        }


        //delete even columns
        for (int i = 0; i < 100; i += 2) {
            ColumnPath columnPath = newColumnPath("Standard1", null, "col" + i);
            clientLibrary.remove(key1, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key2, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+1] = clientLibrary.getContext().getDeps().iterator().next();

            clientLibrary.remove(key3, columnPath);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            removedDeps[i*3+2] = clientLibrary.getContext().getDeps().iterator().next();
        }

        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.transactional_multiget_count(keys, par, FULL_SLICE_PREDICATE);
        assertEquals(0, keyToCount.get(key1).intValue());
        assertEquals(0, keyToCount.get(key2).intValue());
        assertEquals(0, keyToCount.get(key3).intValue());
        assertEquals(300, clientLibrary.getContext().getDeps().size());
        for (int i = 0; i < 300; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
        }

        //get 10 results from the middle of the range
        clientLibrary.getContext().clearDeps();
        keyToCount = clientLibrary.transactional_multiget_count(keys, par, slice50s);
        assertEquals(0, keyToCount.get(key1).intValue());
        assertEquals(0, keyToCount.get(key2).intValue());
        assertEquals(0, keyToCount.get(key3).intValue());
        assertEquals(30, clientLibrary.getContext().getDeps().size());
        for (int i = 50*3; i < 59*3; i++) {
            assertTrue(clientLibrary.getContext().getDeps().contains(removedDeps[i]));
        }


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testTransactionalMultiGetCount passed");
    }


    @Test
    public void testGetRangeSlices()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = ByteBufferUtil.bytes("1");
        ByteBuffer key2 = ByteBufferUtil.bytes("2");
        ByteBuffer key3 = ByteBufferUtil.bytes("3");
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        //WL TODO use the keyRangeOfAllKeys in some tests ... this requires moving back to ByteOrderedParitioner for it to make sense though
        //KeyRange keyRangeOfAllKeys = new KeyRange().setStart_key(ByteBufferUtil.bytes("0")).setEnd_key(ByteBufferUtil.bytes("4"));
        KeyRange keyRangeOfAllTokens = new KeyRange().setStart_token("0").setEnd_token("0");
        //WL TODO: Test secondary indexes with IndexExpression
        //KeyRange keyRangeOfAllFilter = new KeyRange().setRow_filter(new IndexExpression("col0", IndexOperator.GT, )
        ColumnParent par = new ColumnParent("Standard1");

        clientLibrary.insert(key1, par, newColumn("col1", "val1"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col2", "val2"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col3", "val3"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col1", "val4"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep4 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col2", "val5"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col1", "val6"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col2", "val7"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7 = clientLibrary.getContext().getDeps().iterator().next();


        //test slice of 1 for all 3 keys
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfOne = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col1")));

        List<KeySlice> rangeSliceResults = clientLibrary.get_range_slices(par, sliceOfOne, keyRangeOfAllTokens);
        assertEquals(3, rangeSliceResults.size());
        assertEquals(1, rangeSliceResults.get(0).columns.size());
        assertEquals(1, rangeSliceResults.get(1).columns.size());
        assertEquals(1, rangeSliceResults.get(2).columns.size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));


        //test slice of everything
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfThree = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col4")));
        rangeSliceResults = clientLibrary.get_range_slices(par, sliceOfThree, keyRangeOfAllTokens);
        assertEquals(3, rangeSliceResults.size());
        assertEquals(7, rangeSliceResults.get(0).columns.size() + rangeSliceResults.get(1).columns.size() + rangeSliceResults.get(2).columns.size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7));


        //test slice of 3 deleted down to 1 result, slice of 2 deleted down to 1 result, and slice of 2 deleted down to 0
        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
        ColumnPath standard1_col2 = newColumnPath("Standard1", null, "col2");
        ColumnPath standard1_col3 = newColumnPath("Standard1", null, "col3");

        clientLibrary.remove(key1, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key1, standard1_col3);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key2, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col1);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        rangeSliceResults = clientLibrary.get_range_slices(par, sliceOfThree, keyRangeOfAllTokens);
        assertEquals(3, rangeSliceResults.size());
        assertEquals(2, rangeSliceResults.get(0).columns.size() + rangeSliceResults.get(1).columns.size() + rangeSliceResults.get(2).columns.size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7removed));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testGetRangeSlices passed");
    }

    //WL TODO: Robustly test transactional_get_range_slices using concurrent writes
    @Test
    public void testTransactionalGetRangeSlices()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = ByteBufferUtil.bytes("1");
        ByteBuffer key2 = ByteBufferUtil.bytes("2");
        ByteBuffer key3 = ByteBufferUtil.bytes("3");
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        //KeyRange keyRangeOfAllKeys = new KeyRange().setStart_key(ByteBufferUtil.bytes("0")).setEnd_key(ByteBufferUtil.bytes("4"));
        KeyRange keyRangeOfAllTokens = new KeyRange().setStart_token("0").setEnd_token("0");
        //WL TODO: Test secondary indexes with IndexExpression
        //KeyRange keyRangeOfAllFilter = new KeyRange().setRow_filter(new IndexExpression("col0", IndexOperator.GT, )
        ColumnParent par = new ColumnParent("Standard1");

        clientLibrary.insert(key1, par, newColumn("col1", "val1"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col2", "val2"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col3", "val3"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col1", "val4"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep4 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col2", "val5"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col1", "val6"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col2", "val7"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7 = clientLibrary.getContext().getDeps().iterator().next();


        //test slice of 1 for all 3 keys
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfOne = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col1")));

        List<KeySlice> rangeSliceResults = clientLibrary.transactional_get_range_slices(par, sliceOfOne, keyRangeOfAllTokens);
        assertEquals(3, rangeSliceResults.size());
        assertEquals(1, rangeSliceResults.get(0).columns.size());
        assertEquals(1, rangeSliceResults.get(1).columns.size());
        assertEquals(1, rangeSliceResults.get(2).columns.size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));


        //test slice of everything
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfThree = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col3")));
        rangeSliceResults = clientLibrary.transactional_get_range_slices(par, sliceOfThree, keyRangeOfAllTokens);
        assertEquals(3, rangeSliceResults.size());
        assertEquals(7, rangeSliceResults.get(0).columns.size() + rangeSliceResults.get(1).columns.size() + rangeSliceResults.get(2).columns.size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7));


        //test slice of 3 deleted down to 1 result, slice of 2 deleted down to 1 result, and slice of 2 deleted down to 0
        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
        ColumnPath standard1_col2 = newColumnPath("Standard1", null, "col2");
        ColumnPath standard1_col3 = newColumnPath("Standard1", null, "col3");

        clientLibrary.remove(key1, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key1, standard1_col3);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key2, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col1);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        rangeSliceResults = clientLibrary.transactional_get_range_slices(par, sliceOfThree, keyRangeOfAllTokens);
        assertEquals(3, rangeSliceResults.size());
        assertEquals(2, rangeSliceResults.get(0).columns.size() + rangeSliceResults.get(1).columns.size() + rangeSliceResults.get(2).columns.size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7removed));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testTransactionalGetRangeSlices passed");
    }

    @Test
    public void testAddGet()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id = randomKey();
        ColumnParent counter1 = new ColumnParent("Counter1");
        ColumnParent supercounter1_supercol1 = newColumnParent("SuperCounter1", "SuperCol1");
        supercounter1_supercol1.setSuper_column(ByteBufferUtil.bytes("SuperCol1"));
        ColumnPath counter1_col1 = newColumnPath("Counter1", null, "col1");
        ColumnPath supercounter1_supercol1_col2 = newColumnPath("SuperCounter1", "SuperCol1", "col2");

        ColumnPath supercounter1_supercol1_col3 = newColumnPath("SuperCounter1", "SuperCol1", "col3");

        clientLibrary.add(key_user_id, counter1, newCounterColumn("col1", 1));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col2", 10));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col3", 100));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();


        clientLibrary.getContext().clearDeps();

        ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, counter1_col1);
        assertNotNull("Got a null ColumnOrSuperColumn", got1);
        assertEquals(1, got1.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        HashSet<Dep> test_deps = new HashSet<Dep>();
        test_deps.add(dep1);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, supercounter1_supercol1_col2);
        assertNotNull("Got a null ColumnOrSuperColumn", got2);
        assertEquals(10, got2.getCounter_column().value);
        assertEquals(2, clientLibrary.getContext().getDeps().size());
        test_deps.add(dep2);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());

        ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id, supercounter1_supercol1_col3);
        assertNotNull("Got a null ColumnOrSuperColumn", got3);
        assertEquals(100, got3.getCounter_column().value);
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        test_deps.add(dep3);
        assertEquals(test_deps, clientLibrary.getContext().getDeps());


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testAddGet passed");
    }

    @Test
    public void testAddGet2()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id = randomKey();
        ColumnParent par = new ColumnParent("Counter1");
        ColumnPath cp = new ColumnPath("Counter1");


        clientLibrary.add(key_user_id, par, newCounterColumn("col1", 1));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, cp);
        assertNotNull("Got a null ColumnOrSuperColumn", got1);
        assertEquals(1, got1.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());


        clientLibrary.add(key_user_id, par, newCounterColumn("col1", 10));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, cp);
        assertNotNull("Got a null ColumnOrSuperColumn", got2);
        assertEquals(11, got2.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2, clientLibrary.getContext().getDeps().iterator().next());


        clientLibrary.add(key_user_id, par, newCounterColumn("col1", 100));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id, cp);
        assertNotNull("Got a null ColumnOrSuperColumn", got3);
        assertEquals(111, got3.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3, clientLibrary.getContext().getDeps().iterator().next());


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testAddGet2 passed");
    }

    @Test
    public void testAddGet3()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        //Same as testAddGet2 except with a supercolumn in the path

        ByteBuffer key_user_id = randomKey();
        ColumnParent par = newColumnParent("SuperCounter1", "SuperCol1");
        ColumnPath cp = newColumnPath("SuperCounter1", "SuperCol1", "col1");


        clientLibrary.add(key_user_id, par, newCounterColumn("col1", 1));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, cp);
        assertNotNull("Got a null ColumnOrSuperColumn", got1);
        assertEquals(1, got1.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());


        clientLibrary.add(key_user_id, par, newCounterColumn("col1", 10));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, cp);
        assertNotNull("Got a null ColumnOrSuperColumn", got2);
        assertEquals(11, got2.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2, clientLibrary.getContext().getDeps().iterator().next());


        clientLibrary.add(key_user_id, par, newCounterColumn("col1", 100));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        cp.column = ByteBufferUtil.bytes("col1");
        ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id, cp);
        assertNotNull("Got a null ColumnOrSuperColumn", got3);
        assertEquals(111, got3.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3, clientLibrary.getContext().getDeps().iterator().next());


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testAddGet3 passed");
    }

    @Test
    public void testAddGetRemoveCounter()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id = randomKey();
        ColumnParent counter1 = new ColumnParent("Counter1");
        ColumnParent supercounter1_supercol1 = newColumnParent("SuperCounter1", "SuperCol1");
        supercounter1_supercol1.setSuper_column(ByteBufferUtil.bytes("SuperCol1"));
        ColumnPath counter1_col1 = newColumnPath("Counter1", null, "col1");
        ColumnPath supercounter1_supercol1_col2 = newColumnPath("SuperCounter1", "SuperCol1", "col2");

        clientLibrary.add(key_user_id, counter1, newCounterColumn("col1", 1));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col2", 10));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();
        clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col2", 100));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();


        clientLibrary.remove_counter(key_user_id, counter1_col1);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove_counter(key_user_id, supercounter1_supercol1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2removed = clientLibrary.getContext().getDeps().iterator().next();


        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, counter1_col1);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1removed, clientLibrary.getContext().getDeps().iterator().next());

        clientLibrary.getContext().clearDeps();
        try {
            ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, supercounter1_supercol1_col2);
            fail("get on a removed ColumnFamily should throw an exception");
        } catch (NotFoundException notFoundException) {
            //expected
        }
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2removed, clientLibrary.getContext().getDeps().iterator().next());


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testAddGetRemoveCounter passed");
    }

    @Test
    public void testAddGet100()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key_user_id = randomKey();
        ColumnParent counter1 = new ColumnParent("Counter1");
        ColumnParent supercounter1_supercol1 = newColumnParent("SuperCounter1", "SuperCol1");
        supercounter1_supercol1.setSuper_column(ByteBufferUtil.bytes("SuperCol1"));
        ColumnPath counter1_col1 = newColumnPath("Counter1", null, "col1");
        ColumnPath supercounter1_supercol1_col2 = newColumnPath("SuperCounter1", "SuperCol1", "col2");

        ColumnPath supercounter1_supercol1_col3 = newColumnPath("SuperCounter1", "SuperCol1", "col3");

        Dep dep1 = null;
        Dep dep2 = null;
        Dep dep3 = null;
        for (int i = 0; i < 100; i++) {
            clientLibrary.add(key_user_id, counter1, newCounterColumn("col1", 1));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            dep1 = clientLibrary.getContext().getDeps().iterator().next();
            clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col2", 10));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            dep2 = clientLibrary.getContext().getDeps().iterator().next();
            clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col3", 100));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            dep3 = clientLibrary.getContext().getDeps().iterator().next();
        }

        clientLibrary.getContext().clearDeps();
        ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, counter1_col1);
        assertNotNull("Got a null ColumnOrSuperColumn", got1);
        assertEquals(100, got1.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());

        clientLibrary.getContext().clearDeps();
        ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, supercounter1_supercol1_col2);
        assertNotNull("Got a null ColumnOrSuperColumn", got2);
        assertEquals(1000, got2.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep2, clientLibrary.getContext().getDeps().iterator().next());

        clientLibrary.getContext().clearDeps();
        ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id, supercounter1_supercol1_col3);
        assertNotNull("Got a null ColumnOrSuperColumn", got3);
        assertEquals(10000, got3.getCounter_column().value);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        assertEquals(dep3, clientLibrary.getContext().getDeps().iterator().next());

        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testAddGet100 passed");
    }

    @Test
    public void testAddMultiDC()
    throws Exception
    {
        if (dcToServerIPAndPorts == null || dcToServerIPAndPorts.size() <= 1) {
            System.out.println("testAddMultiDC SKIPPED");
            return;
        }

        List<ClientLibrary> dcToClient = new ArrayList<ClientLibrary>();
        for (Map<String, Integer> serverIpAndPorts : dcToServerIPAndPorts) {
            dcToClient.add(new ClientLibrary(serverIpAndPorts, "Keyspace1", consistencyLevel));
        }


        ByteBuffer key_user_id = randomKey();
        ColumnParent counter1 = new ColumnParent("Counter1");
        ColumnParent supercounter1_supercol1 = newColumnParent("SuperCounter1", "SuperCol1");
        supercounter1_supercol1.setSuper_column(ByteBufferUtil.bytes("SuperCol1"));
        ColumnPath counter1_col1 = newColumnPath("Counter1", null, "col1");
        ColumnPath supercounter1_supercol1_col2 = newColumnPath("SuperCounter1", "SuperCol1", "col2");
        ColumnPath supercounter1_supercol1_col3 = newColumnPath("SuperCounter1", "SuperCol1", "col3");

        List<Dep> dcToDep1 = new ArrayList<Dep>();
        List<Dep> dcToDep2 = new ArrayList<Dep>();
        List<Dep> dcToDep3 = new ArrayList<Dep>();
        int col1Total = 0;
        int col2Total = 0;
        int col3Total = 0;
        for (int dc = 0; dc < dcToClient.size(); ++dc) {
            ClientLibrary clientLibrary = dcToClient.get(dc);

            int col1Val = 1+dc;
            col1Total += col1Val;
            clientLibrary.add(key_user_id, counter1, newCounterColumn("col1", col1Val));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            dcToDep1.add(clientLibrary.getContext().getDeps().iterator().next());

            int col2Val = (1+dc)*10;
            col2Total += col2Val;
            clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col2", col2Val));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            dcToDep2.add(clientLibrary.getContext().getDeps().iterator().next());

            int col3Val = (1+dc)*100;
            col3Total += col3Val;
            clientLibrary.add(key_user_id, supercounter1_supercol1, newCounterColumn("col3", col3Val));
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            dcToDep3.add(clientLibrary.getContext().getDeps().iterator().next());
        }

        //let things propagate between the "datacenters"
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            //ignored -- probably should make sure we actually slept a decent amount of time
        }

        for (int dc = 0; dc < dcToClient.size(); ++dc) {
            ClientLibrary clientLibrary = dcToClient.get(dc);

            clientLibrary.getContext().clearDeps();

            ColumnOrSuperColumn got1 = clientLibrary.get(key_user_id, counter1_col1);
            assertNotNull("Got a null ColumnOrSuperColumn", got1);
            assertEquals(col1Total, got1.getCounter_column().value);
            assertEquals(dcToClient.size(), clientLibrary.getContext().getDeps().size());
            HashSet<Dep> test_deps = new HashSet<Dep>();
            test_deps.addAll(dcToDep1);
            assertEquals(test_deps, clientLibrary.getContext().getDeps());

            ColumnOrSuperColumn got2 = clientLibrary.get(key_user_id, supercounter1_supercol1_col2);
            assertNotNull("Got a null ColumnOrSuperColumn", got2);
            assertEquals(col2Total, got2.getCounter_column().value);
            assertEquals(2*dcToClient.size(), clientLibrary.getContext().getDeps().size());
            test_deps.addAll(dcToDep2);
            assertEquals(test_deps, clientLibrary.getContext().getDeps());

            ColumnOrSuperColumn got3 = clientLibrary.get(key_user_id, supercounter1_supercol1_col3);
            assertNotNull("Got a null ColumnOrSuperColumn", got3);
            assertEquals(col3Total, got3.getCounter_column().value);
            assertEquals(3*dcToClient.size(), clientLibrary.getContext().getDeps().size());
            test_deps.addAll(dcToDep3);
            assertEquals(test_deps, clientLibrary.getContext().getDeps());
        }

        //cleanup
        dcToClient.get(0).getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testAddMultiDC passed");
    }

    // Identical to the MultigetSlice test, except it uses transactional multiget (should always be 1 round)
    @Test
    public void testTransactionalMultigetNormal()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        ByteBuffer key1 = randomKey();
        ByteBuffer key2 = randomKey();
        ByteBuffer key3 = randomKey();
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        ColumnParent par = new ColumnParent("Standard1");

        //test

        clientLibrary.insert(key1, par, newColumn("col1", "val1"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col2", "val2"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key1, par, newColumn("col3", "val3"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col1", "val4"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep4 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key2, par, newColumn("col2", "val5"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col1", "val6"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6 = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.insert(key3, par, newColumn("col2", "val7"));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7 = clientLibrary.getContext().getDeps().iterator().next();


        //test slice of 1 for all 3 keys
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfOne = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col1")));

        Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne);
        assertEquals(3, multigetResults.size());
        assertEquals(1, multigetResults.get(key1).size());
        assertEquals(1, multigetResults.get(key2).size());
        assertEquals(1, multigetResults.get(key3).size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));


        //test slice of everything
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfThree = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col3")));
        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfThree);
        assertEquals(3, multigetResults.size());
        assertEquals(3, multigetResults.get(key1).size());
        assertEquals(2, multigetResults.get(key2).size());
        assertEquals(2, multigetResults.get(key3).size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7));


        //test slice of 3 deleted down to 1 result, slice of 2 deleted down to 1 result, and slice of 2 deleted down to 0
        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
        ColumnPath standard1_col2 = newColumnPath("Standard1", null, "col2");
        ColumnPath standard1_col3 = newColumnPath("Standard1", null, "col3");

        clientLibrary.remove(key1, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep2removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key1, standard1_col3);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep3removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key2, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep5removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col1);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep6removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.remove(key3, standard1_col2);
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep7removed = clientLibrary.getContext().getDeps().iterator().next();

        clientLibrary.getContext().clearDeps();
        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfThree);
        assertEquals(3, multigetResults.size());
        assertEquals(1, multigetResults.get(key1).size());
        assertEquals(1, multigetResults.get(key2).size());
        assertEquals(0, multigetResults.get(key3).size());
        assertEquals(7, clientLibrary.getContext().getDeps().size());
        assertTrue(clientLibrary.getContext().getDeps().contains(dep1));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep2removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep3removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep4));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep5removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep6removed));
        assertTrue(clientLibrary.getContext().getDeps().contains(dep7removed));


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testTransactionalMultigetNormal passed");
    }

    @Test
    public void testTransactionalMultigetConcurrent()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        //test transactional multiget using the concurrent_write_callback

        ByteBuffer key1 = randomKey();
        ByteBuffer key2 = randomKey();
        ByteBuffer key3 = randomKey();
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        ColumnParent par = new ColumnParent("Standard1");


        //test with concurrent writes that update c so that key2/col1 and key3/col1 depend on
        //a newer version of key1/col1 than was gotten in the first round
        clientLibrary.insert(key1, par, newColumn("col1", "1.1"));
        clientLibrary.insert(key2, par, newColumn("col1", "2.1"));
        clientLibrary.insert(key3, par, newColumn("col1", "3.1"));


        //test w/ 1, 2, and 10 concurrent overwrites
        clientLibrary.getContext().clearDeps();
        SlicePredicate sliceOfOne = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col1")));

        class OverwriteNTimes implements CopsTestingConcurrentWriteHook
        {
            ClientLibrary clientLibrary;
            Collection<ByteBuffer> keys;
            ColumnParent par;
            String columnName;
            int value = 0;
            int nTimes;

            public OverwriteNTimes(ClientLibrary clientLibrary, ColumnParent par, Collection<ByteBuffer> keys, String columnName, int nTimes) {
                this.clientLibrary = clientLibrary;
                this.keys = keys;
                this.par = par;
                this.columnName = columnName;
                this.nTimes = nTimes;
            }

            @Override
            public void issueWrites()
            {
                try {
                    for (int i = 0; i < nTimes; ++i) {
                        for (ByteBuffer key : keys) {
                            clientLibrary.insert(key, par, newColumn(columnName, String.valueOf(value++)));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("ERROR in overwriteNTimes: " + e);
                }
            }
        }

        //test w/ 1 concurrent overwrite: testing get_trans algorithm, but not old version support on the serverside
        clientLibrary.getContext().clearDeps();
        OverwriteNTimes overwriteOnce = new OverwriteNTimes(clientLibrary, par, keys, "col1", 1);
        Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, overwriteOnce, null);
        assertEquals(3, multigetResults.size());
        assertEquals(1, multigetResults.get(key1).size());
        assertEquals("0", ByteBufferUtil.string(multigetResults.get(key1).iterator().next().column.value));
        assertEquals(1, multigetResults.get(key2).size());
        assertEquals("1", ByteBufferUtil.string(multigetResults.get(key2).iterator().next().column.value));
        assertEquals(1, multigetResults.get(key3).size());
        assertEquals("2", ByteBufferUtil.string(multigetResults.get(key3).iterator().next().column.value));
        assertEquals(3, clientLibrary.getContext().getDeps().size());

        //test w/ 2 concurrent overwrites: testing get_trans algorithm, and old version support on the serverside
        clientLibrary.getContext().clearDeps();
        OverwriteNTimes overwriteTwice = new OverwriteNTimes(clientLibrary, par, keys, "col1", 2);
        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, overwriteTwice, overwriteTwice);
        assertEquals(3, multigetResults.size());
        assertEquals(1, multigetResults.get(key1).size());
        assertEquals("3", ByteBufferUtil.string(multigetResults.get(key1).iterator().next().column.value));
        assertEquals(1, multigetResults.get(key2).size());
        assertEquals("4", ByteBufferUtil.string(multigetResults.get(key2).iterator().next().column.value));
        assertEquals(1, multigetResults.get(key3).size());
        assertEquals("5", ByteBufferUtil.string(multigetResults.get(key3).iterator().next().column.value));
        assertEquals(3, clientLibrary.getContext().getDeps().size());

        //test w/ 10 concurrent overwrites: testing get_trans algorithm, and old version support on the serverside
        clientLibrary.getContext().clearDeps();
        OverwriteNTimes overwriteTenTimes = new OverwriteNTimes(clientLibrary, par, keys, "col1", 10);
        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, overwriteTenTimes, overwriteTenTimes);
        assertEquals(3, multigetResults.size());
        assertEquals(1, multigetResults.get(key1).size());
        assertEquals("27", ByteBufferUtil.string(multigetResults.get(key1).iterator().next().column.value));
        assertEquals(1, multigetResults.get(key2).size());
        assertEquals("28", ByteBufferUtil.string(multigetResults.get(key2).iterator().next().column.value));
        assertEquals(1, multigetResults.get(key3).size());
        assertEquals("29", ByteBufferUtil.string(multigetResults.get(key3).iterator().next().column.value));
        assertEquals(3, clientLibrary.getContext().getDeps().size());


        class OverwriteNTimesThenRemove extends OverwriteNTimes
        {
            ColumnPath columnPath;

            public OverwriteNTimesThenRemove(ClientLibrary clientLibrary, ColumnParent par, Collection<ByteBuffer> keys, String columnName, int nTimes) {
                super(clientLibrary, par, keys, columnName, nTimes);
                if (par.isSetSuper_column()) {
                    this.columnPath = new ColumnPath(par.column_family).setSuper_column(par.super_column).setColumn(ByteBufferUtil.bytes(columnName));
                } else {
                    this.columnPath = new ColumnPath(par.column_family).setColumn(ByteBufferUtil.bytes(columnName));
                }
            }

            @Override
            public void issueWrites()
            {
                //OverwriteNTimes
                super.issueWrites();

                //ThenRemove
                try {
                    for (ByteBuffer key : keys) {
                        clientLibrary.remove(key, columnPath);
                    }
                } catch (Exception e) {
                    System.err.println("ERROR in overwriteNTimes: " + e);
                }
            }
        }


          //NOTE: concurrently deleting one column isn't a legitimate test -- a consistent view might OR might not include the delete, so we can't use this test
//        //test w/ concurrent delete of one column
//        clientLibrary.getContext().clearDeps();
//        OverwriteNTimesThenRemove removeKey2 = new OverwriteNTimesThenRemove(clientLibrary, par, Collections.singletonList(key2), "col1", 0);
//        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, removeKey2, null);
//        assertEquals(3, multigetResults.size());
//        assertEquals(1, multigetResults.get(key1).size());
//        assertEquals(0, multigetResults.get(key2).size());
//        assertEquals(1, multigetResults.get(key3).size());
//        assertEquals(3, clientLibrary.getContext().getDeps().size());

        //test w/ concurrent update, then delete of all columns
        clientLibrary.getContext().clearDeps();
        OverwriteNTimesThenRemove overwriteOnceThenRemove = new OverwriteNTimesThenRemove(clientLibrary, par, keys, "col1", 1);
        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, overwriteOnceThenRemove, null);
        assertEquals(3, multigetResults.size());
        assertEquals(0, multigetResults.get(key1).size());
        assertEquals(0, multigetResults.get(key2).size());
        assertEquals(0, multigetResults.get(key3).size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());

        //test w/ concurrent update, then delete of all columns then overwrite again (tests old version support of deleted columns)
        clientLibrary.getContext().clearDeps();
        OverwriteNTimesThenRemove overwriteOnceThenRemove2 = new OverwriteNTimesThenRemove(clientLibrary, par, keys, "col1", 1);
        OverwriteNTimes overwriteTenTimes2 = new OverwriteNTimes(clientLibrary, par, keys, "col1", 10);
        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, overwriteOnceThenRemove2, overwriteTenTimes2);
        assertEquals(3, multigetResults.size());
        assertEquals(0, multigetResults.get(key1).size());
        assertEquals(0, multigetResults.get(key2).size());
        assertEquals(0, multigetResults.get(key3).size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());

        //test w/ concurrent update, then delete of all columns then overwrite again then removed again (tests old version support of deleted columns)
        clientLibrary.getContext().clearDeps();
        OverwriteNTimesThenRemove overwriteOnceThenRemove3 = new OverwriteNTimesThenRemove(clientLibrary, par, keys, "col1", 1);
        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, overwriteOnceThenRemove3, overwriteOnceThenRemove3);
        assertEquals(3, multigetResults.size());
        assertEquals(0, multigetResults.get(key1).size());
        assertEquals(0, multigetResults.get(key2).size());
        assertEquals(0, multigetResults.get(key3).size());
        assertEquals(3, clientLibrary.getContext().getDeps().size());


        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testTransactionalMultigetConcurrent passed");
    }

    // TODO: Reenable and implement proper old version handling for counter columns
//     @Test
//     public void testTransactionalMultigetCounterColumn()
//     throws Exception
//     {
//         ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

//         //test transactional multiget using the concurrent_write_callback

//         ByteBuffer key1 = randomKey();
//         ByteBuffer key2 = randomKey();
//         ByteBuffer key3 = randomKey();
//         ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
//         keys.add(key1);
//         keys.add(key2);
//         keys.add(key3);
//         ColumnParent par = new ColumnParent("Counter1");


//         //test with concurrent writes that update c so that key2/col1 and key3/col1 depend on
//         //a newer version of key1/col1 than was gotten in the first round
//         clientLibrary.add(key1, par, newCounterColumn("col1", 0));
//         clientLibrary.add(key2, par, newCounterColumn("col1", 0));
//         clientLibrary.add(key3, par, newCounterColumn("col1", 0));


//         //test w/ 1, 2, and 10 concurrent overwrites
//         clientLibrary.getContext().clearDeps();
//         SlicePredicate sliceOfOne = new SlicePredicate().setSlice_range(new SliceRange().setStart(ByteBufferUtil.bytes("col1")).setFinish(ByteBufferUtil.bytes("col1")));

//         class IncrementNTimes implements CopsTestingConcurrentWriteHook
//         {
//             ClientLibrary clientLibrary;
//             Collection<ByteBuffer> keys;
//             ColumnParent par;
//             String columnName;
//             int nTimes;

//             public IncrementNTimes(ClientLibrary clientLibrary, ColumnParent par, Collection<ByteBuffer> keys, String columnName, int nTimes) {
//                 this.clientLibrary = clientLibrary;
//                 this.keys = keys;
//                 this.par = par;
//                 this.columnName = columnName;
//                 this.nTimes = nTimes;
//             }

//             @Override
//             public void issueWrites()
//             {
//                 try {
//                     for (int i = 0; i < nTimes; ++i) {
//                         for (ByteBuffer key : keys) {
//                             clientLibrary.add(key, par, newCounterColumn(columnName, 1));
//                         }
//                     }
//                 } catch (Exception e) {
//                     System.err.println("ERROR in incrementNTimes: " + e);
//                 }
//             }
//         }

//         //test w/ 1 concurrent increment: testing get_trans algorithm, but not old version support on the serverside
//         clientLibrary.getContext().clearDeps();
//         IncrementNTimes incrementOnce = new IncrementNTimes(clientLibrary, par, keys, "col1", 1);
//         Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, incrementOnce, null);
//         assertEquals(3, multigetResults.size());
//         assertEquals(1, multigetResults.get(key1).size());
//         assertEquals(1, multigetResults.get(key1).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key2).size());
//         assertEquals(1, multigetResults.get(key2).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key3).size());
//         assertEquals(1, multigetResults.get(key3).iterator().next().counter_column.value);
//         assertEquals(3, clientLibrary.getContext().getDeps().size());

//         //test w/ 100 concurrent increments: testing get_trans algorithm, but not old version support on the serverside
//         clientLibrary.getContext().clearDeps();
//         IncrementNTimes incrementHundredTimes = new IncrementNTimes(clientLibrary, par, keys, "col1", 100);
//         multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, incrementHundredTimes, null);
//         assertEquals(3, multigetResults.size());
//         assertEquals(1, multigetResults.get(key1).size());
//         assertEquals(101, multigetResults.get(key1).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key2).size());
//         assertEquals(101, multigetResults.get(key2).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key3).size());
//         assertEquals(101, multigetResults.get(key3).iterator().next().counter_column.value);
//         assertEquals(3, clientLibrary.getContext().getDeps().size());

//         //test w/ 2 concurrent increments: testing get_trans algorithm, and old version support on the serverside
//         clientLibrary.getContext().clearDeps();
//         IncrementNTimes incrementTwice = new IncrementNTimes(clientLibrary, par, keys, "col1", 2);
//         multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, incrementTwice, incrementTwice);
//         assertEquals(3, multigetResults.size());
//         assertEquals(1, multigetResults.get(key1).size());
//         assertEquals(103, multigetResults.get(key1).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key2).size());
//         assertEquals(103, multigetResults.get(key2).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key3).size());
//         assertEquals(103, multigetResults.get(key3).iterator().next().counter_column.value);
//         assertEquals(3, clientLibrary.getContext().getDeps().size());

//         //test w/ 10 concurrent increments: testing get_trans algorithm, and old version support on the serverside
//         clientLibrary.getContext().clearDeps();
//         IncrementNTimes incrementTenTimes = new IncrementNTimes(clientLibrary, par, keys, "col1", 10);
//         multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, incrementTenTimes, incrementTenTimes);
//         assertEquals(3, multigetResults.size());
//         assertEquals(1, multigetResults.get(key1).size());
//         assertEquals(115, multigetResults.get(key1).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key2).size());
//         assertEquals(115, multigetResults.get(key2).iterator().next().counter_column.value);
//         assertEquals(1, multigetResults.get(key3).size());
//         assertEquals(115, multigetResults.get(key3).iterator().next().counter_column.value);
//         assertEquals(3, clientLibrary.getContext().getDeps().size());


//         //WL TODO: Removing and readding countercolumns has some weird properties (like the remove has to propagate everywhere),
//         //and it's a rare cease so I'm skipping this for now
// //        class IncrementNTimesThenRemove extends IncrementNTimes
// //        {
// //            ColumnPath columnPath;
// //
// //            public IncrementNTimesThenRemove(ClientLibrary clientLibrary, ColumnParent par, Collection<ByteBuffer> keys, String columnName, int nTimes) {
// //                super(clientLibrary, par, keys, columnName, nTimes);
// //                if (par.isSetSuper_column()) {
// //                    this.columnPath = new ColumnPath(par.column_family).setSuper_column(par.super_column).setColumn(ByteBufferUtil.bytes(columnName));
// //                } else {
// //                    this.columnPath = new ColumnPath(par.column_family).setColumn(ByteBufferUtil.bytes(columnName));
// //                }
// //            }
// //
// //            @Override
// //            public void issueWrites()
// //            {
// //                //IncrementNTimes
// //                super.issueWrites();
// //
// //                //ThenRemove
// //                try {
// //                    for (ByteBuffer key : keys) {
// //                        clientLibrary.remove_counter_safe(key, columnPath);
// //                    }
// //                } catch (Exception e) {
// //                    System.err.println("ERROR in incrementNTimesThenRemove: " + e);
// //                }
// //            }
// //        }
// //
// //        //NOTE: concurrently deleting one column isn't a legitimate test -- a consistent view might OR might not include the delete, so we can't use this test
// ////        //test w/ concurrent delete of one column
// ////        clientLibrary.getContext().clearDeps();
// ////        IncrementNTimesThenRemove removeKey2 = new IncrementNTimesThenRemove(clientLibrary, par, Collections.singletonList(key2), "col1", 0);
// ////        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, removeKey2, null);
// ////        assertEquals(3, multigetResults.size());
// ////        assertEquals(1, multigetResults.get(key1).size());
// ////        assertEquals(0, multigetResults.get(key2).size());
// ////        assertEquals(1, multigetResults.get(key3).size());
// ////        assertEquals(3, clientLibrary.getContext().getDeps().size());
// //
// //        //test w/ concurrent update, then delete of all columns
// //        clientLibrary.getContext().clearDeps();
// //        IncrementNTimesThenRemove incrementOnceThenRemove = new IncrementNTimesThenRemove(clientLibrary, par, keys, "col1", 1);
// //        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, incrementOnceThenRemove, null);
// //        assertEquals(3, multigetResults.size());
// //        assertEquals(0, multigetResults.get(key1).size());
// //        assertEquals(0, multigetResults.get(key2).size());
// //        assertEquals(0, multigetResults.get(key3).size());
// //        assertEquals(3, clientLibrary.getContext().getDeps().size());
// //
// //        //test w/ concurrent update, then delete of all columns then increment again (tests old version support of deleted columns)
// //        clientLibrary.getContext().clearDeps();
// //        IncrementNTimesThenRemove incrementOnceThenRemove2 = new IncrementNTimesThenRemove(clientLibrary, par, keys, "col1", 1);
// //        IncrementNTimes incrementTenTimes2 = new IncrementNTimes(clientLibrary, par, keys, "col1", 10);
// //        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, incrementOnceThenRemove2, incrementTenTimes2);
// //        assertEquals(3, multigetResults.size());
// //        assertEquals(0, multigetResults.get(key1).size());
// //        assertEquals(0, multigetResults.get(key2).size());
// //        assertEquals(0, multigetResults.get(key3).size());
// //        assertEquals(3, clientLibrary.getContext().getDeps().size());
// //
// //        //test w/ concurrent update, then delete of all columns then increment again then removed again (tests old version support of deleted columns)
// //        clientLibrary.getContext().clearDeps();
// //        IncrementNTimesThenRemove incrementOnceThenRemove3 = new IncrementNTimesThenRemove(clientLibrary, par, keys, "col1", 1);
// //        multigetResults = clientLibrary.transactional_multiget_slice(keys, par, sliceOfOne, incrementOnceThenRemove3, incrementOnceThenRemove3);
// //        assertEquals(3, multigetResults.size());
// //        assertEquals(0, multigetResults.get(key1).size());
// //        assertEquals(0, multigetResults.get(key2).size());
// //        assertEquals(0, multigetResults.get(key3).size());
// //        assertEquals(3, clientLibrary.getContext().getDeps().size());


//         //cleanup
//         clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

//         System.out.println("testTransactionalMultigetCounterColumn passed");
//     }

   @Test
   public void testTransactionalBatchMutateOfOne()
   throws Exception
   {
       ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

       ByteBuffer key1 = randomKey();

       ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
       ColumnPath super1_supercol1_column02 = newColumnPath("Super1", "SuperCol1", "column02");
       ColumnPath super1_supercol2 = newColumnPath("Super1", "SuperCol2", null);
       ArrayList<Column> oneColumnList = new ArrayList<Column>();
       ArrayList<ByteBuffer> oneColumnNameList = new ArrayList<ByteBuffer>();
       //column names must be 8 bytes in a super column
       oneColumnList.add(newColumn("column02", "value2"));
       oneColumnNameList.add(oneColumnList.get(0).name);

       ArrayList<Column> twoColumnList = new ArrayList<Column>();
       ArrayList<ByteBuffer> twoColumnNameList = new ArrayList<ByteBuffer>();
       //column names must be 8 bytes in a super column
       twoColumnList.add(newColumn("column03", "value3"));
       twoColumnList.add(newColumn("column04", "value4"));
       twoColumnNameList.add(twoColumnList.get(0).name);
       twoColumnNameList.add(twoColumnList.get(1).name);

       //Relatively exhaustive set of mutations.
       Mutation insertColumnMutation = newMutation(newColumn("col1", "value1"));
       Mutation insertSuperColumnOfOneMutation = newMutation(newSuperColumn("SuperCol1", oneColumnList));
       Mutation insertSuperColumnOfTwoMutation = newMutation(newSuperColumn("SuperCol2", twoColumnList));


       //WL TODO: Add support for transactionally deleting entire column families and super columns
       //Mutation deleteColumnFamilyMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L));
       //Mutation deleteSuperColumnOfOneMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol1")));
       //Mutation deleteSuperColumnOfTwoMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol2")));

       Mutation deleteSlicePredicateOfOneNameMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol1")).setPredicate(new SlicePredicate().setColumn_names(oneColumnNameList)));
       Mutation deleteSlicePredicateOfTwoNamesMutation = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setSuper_column(ByteBufferUtil.bytes("SuperCol2")).setPredicate(new SlicePredicate().setColumn_names(twoColumnNameList)));
       //Cassandra does not yet support deletions of slice ranges, so we can't test it -- that would also make things quite tricky!
       //Mutation deleteFullSliceRange = new Mutation().setDeletion(new Deletion().setTimestamp(0L).setPredicate(new SlicePredicate().setSlice_range(FULL_SLICE_RANGE)));


       //Test each type of mutation

       // Insert Column
       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Standard1", insertColumnMutation));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

       ColumnOrSuperColumn got1 = clientLibrary.get(key1, standard1_col1);
       assertEquals("value1", ByteBufferUtil.string(got1.column.value));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());

//       // Delete whole Column Family
//       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Standard1", deleteColumnFamilyMutation));
//       assertEquals(1, clientLibrary.getContext().getDeps().size());
//       Dep dep1DeleteCF = clientLibrary.getContext().getDeps().iterator().next();
//
//       clientLibrary.getContext().clearDeps();
//       try {
//           ColumnOrSuperColumn got1DeleteCF = clientLibrary.get(key1, standard1_col1);
//           fail("get on a removed ColumnFamily should throw an exception");
//       } catch (NotFoundException notFoundException) {
//           //expected
//       }
//       assertEquals(1, clientLibrary.getContext().getDeps().size());
//       assertEquals(dep1DeleteCF, clientLibrary.getContext().getDeps().iterator().next());

       //Insert super column with one column
       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfOneMutation));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       Dep dep2 = clientLibrary.getContext().getDeps().iterator().next();

       ColumnOrSuperColumn got2 = clientLibrary.get(key1, super1_supercol1_column02);
       assertEquals("value2", ByteBufferUtil.string(got2.column.value));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep2, clientLibrary.getContext().getDeps().iterator().next());

//       System.out.println(3);
//       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", deleteSuperColumnOfOneMutation));
//       assertEquals(1, clientLibrary.getContext().getDeps().size());
//       Dep dep2DeleteSC = clientLibrary.getContext().getDeps().iterator().next();
//
//       clientLibrary.getContext().clearDeps();
//       try {
//           ColumnOrSuperColumn got2DeleteSC = clientLibrary.get(key1, super1_supercol1_column02);
//           fail("get on a removed ColumnFamily should throw an exception");
//       } catch (NotFoundException notFoundException) {
//           //expected
//       }
//       assertEquals(1, clientLibrary.getContext().getDeps().size());
//       assertEquals(dep2DeleteSC, clientLibrary.getContext().getDeps().iterator().next());

       // insert again, so we can try the other delete type
       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfOneMutation));
       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", deleteSlicePredicateOfOneNameMutation));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       Dep dep2DeleteSP = clientLibrary.getContext().getDeps().iterator().next();

       clientLibrary.getContext().clearDeps();
       try {
           ColumnOrSuperColumn got2DeleteSP = clientLibrary.get(key1, super1_supercol1_column02);
           fail("get on a removed ColumnFamily should throw an exception");
       } catch (NotFoundException notFoundException) {
           //expected
       }
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep2DeleteSP, clientLibrary.getContext().getDeps().iterator().next());


       //Insert super column with two columns
       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfTwoMutation));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       Dep dep3 = clientLibrary.getContext().getDeps().iterator().next();

       ColumnOrSuperColumn got3 = clientLibrary.get(key1, super1_supercol2);
       assertTrue(got3.isSetSuper_column());
       assertEquals(2, got3.super_column.columns.size());
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep3, clientLibrary.getContext().getDeps().iterator().next());

//       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", deleteSuperColumnOfTwoMutation));
//       assertEquals(1, clientLibrary.getContext().getDeps().size());
//       Dep dep3DeleteSC = clientLibrary.getContext().getDeps().iterator().next();
//
//       clientLibrary.getContext().clearDeps();
//       try {
//           ColumnOrSuperColumn got3DeleteSP = clientLibrary.get(key1, super1_supercol2);
//           fail("get on a removed ColumnFamily should throw an exception");
//       } catch (NotFoundException notFoundException) {
//           //expected
//       }
//       assertEquals(1, clientLibrary.getContext().getDeps().size());
//       assertEquals(dep3DeleteSC, clientLibrary.getContext().getDeps().iterator().next());

       // insert again, so we can try the other delete type
       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", insertSuperColumnOfTwoMutation));
       clientLibrary.transactional_batch_mutate(newMutationMap(key1, "Super1", deleteSlicePredicateOfTwoNamesMutation));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       Dep dep3DeleteSP = clientLibrary.getContext().getDeps().iterator().next();

       clientLibrary.getContext().clearDeps();
       try {
           ColumnOrSuperColumn got3DeleteSP = clientLibrary.get(key1, super1_supercol2);
           fail("get on a removed ColumnFamily should throw an exception");
       } catch (NotFoundException notFoundException) {
           //expected
       }
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep3DeleteSP, clientLibrary.getContext().getDeps().iterator().next());


       //cleanup
       clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());
       clientLibrary.getAnyClient().truncate("Super1", new HashSet<Dep>(), LamportClock.sendTimestamp());

       System.out.println("testTransactionalBatchMutateOfOne passed");
   }

   @Test
   public void testTransactionalBatchMutateOfThree()
   throws Exception
   {
       ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);


       ArrayList<ByteBuffer> threeKeys = new ArrayList<ByteBuffer>();
       ByteBuffer key1 = randomKey();
       ByteBuffer key2 = randomKey();
       ByteBuffer key3 = randomKey();
       threeKeys.add(key1);
       threeKeys.add(key2);
       threeKeys.add(key3);

       ArrayList<String> threeColumnFamilies = new ArrayList<String>();
       threeColumnFamilies.add("Standard1");
       threeColumnFamilies.add("Standard1");
       threeColumnFamilies.add("Standard1");

       ArrayList<Mutation> threeMutations = new ArrayList<Mutation>();
       Mutation insertColumnMutation1 = newMutation(newColumn("col1", "value1"));
       Mutation insertColumnMutation2 = newMutation(newColumn("col2", "value2"));
       Mutation insertColumnMutation3 = newMutation(newColumn("col3", "value3"));
       threeMutations.add(insertColumnMutation1);
       threeMutations.add(insertColumnMutation2);
       threeMutations.add(insertColumnMutation3);


       clientLibrary.transactional_batch_mutate(newMutationMap(threeKeys, threeColumnFamilies, threeMutations));
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

       ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");
       ColumnPath standard1_col2 = newColumnPath("Standard1", null, "col2");
       ColumnPath standard1_col3 = newColumnPath("Standard1", null, "col3");

       clientLibrary.getContext().clearDeps();
       clientLibrary.get(key1, standard1_col1);
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());

       clientLibrary.getContext().clearDeps();
       clientLibrary.get(key2, standard1_col2);
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());

       clientLibrary.getContext().clearDeps();
       clientLibrary.get(key3, standard1_col3);
       assertEquals(1, clientLibrary.getContext().getDeps().size());
       assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());


       //cleanup
       clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

       System.out.println("testTransactionalBatchMutateOfThree passed");
   }

    @Test
    public void testTransactionalBatchMutateOfMany()
    throws Exception
    {
        ClientLibrary clientLibrary = new ClientLibrary(localServerIPAndPorts, "Keyspace1", consistencyLevel);

        final int numKeys = 100;

        ArrayList<ByteBuffer> manyKeys = new ArrayList<ByteBuffer>(numKeys);
        ArrayList<String> manyColumnFamilies = new ArrayList<String>(numKeys);
        ArrayList<Mutation> manyMutations = new ArrayList<Mutation>(numKeys);
        for (int i = 0; i < numKeys; ++i) {
            manyKeys.add(randomKey());
            manyColumnFamilies.add("Standard1");
            manyMutations.add(newMutation(newColumn("col1", "value" + i)));
        }

        clientLibrary.transactional_batch_mutate(newMutationMap(manyKeys, manyColumnFamilies, manyMutations));
        assertEquals(1, clientLibrary.getContext().getDeps().size());
        Dep dep1 = clientLibrary.getContext().getDeps().iterator().next();

        ColumnPath standard1_col1 = newColumnPath("Standard1", null, "col1");

        for (int i = 0; i < numKeys; ++i) {
            clientLibrary.getContext().clearDeps();
            clientLibrary.get(manyKeys.get(i), standard1_col1);
            assertEquals(1, clientLibrary.getContext().getDeps().size());
            assertEquals(dep1, clientLibrary.getContext().getDeps().iterator().next());
        }

        //cleanup
        clientLibrary.getAnyClient().truncate("Standard1", new HashSet<Dep>(), LamportClock.sendTimestamp());

        System.out.println("testTransactionalBatchMutateOfMany passed");
    }

    //WL TODO: Add testing of TransactionBatchMutateOfCounterColumns
    //WL TODO: Add testing of TransactionBatchMutate with SuperColumn deletes
    //WL TODO: Add testing of TransactionBatchMutate with ColumnFamily deletes
    //WL TODO: Add testing of TransactionBatchMutate with all operation types
    //WL TODO: Add a more systematic way to ensure transactions executed at remote datacenters

}