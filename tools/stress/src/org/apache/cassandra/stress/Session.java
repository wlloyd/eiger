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
package org.apache.cassandra.stress;

import static com.google.common.base.Charsets.UTF_8;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LamportClock;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Session implements Serializable
{
    // command line options
    public static final Options availableOptions = new Options();

    public static final String DEFAULT_COMPARATOR = "AsciiType";
    public static final String DEFAULT_VALIDATOR  = "BytesType";

    private static InetAddress localInetAddress;

    public final AtomicInteger operations;
    public final AtomicInteger keys;
    public final AtomicInteger columnCount;
    public final AtomicLong    bytes;
    public final AtomicLong    latency;
    public final ConcurrentLinkedQueue<Long> latencies;

    static
    {
        availableOptions.addOption("h",  "help",                 false,  "Show this help message and exit");
        availableOptions.addOption("n",  "num-keys",             true,   "Number of keys, default:1000000");
        availableOptions.addOption("F",  "num-different-keys",   true,   "Number of different keys (if < NUM-KEYS, the same key will re-used multiple times), default:NUM-KEYS");
        availableOptions.addOption("N",  "skip-keys",            true,   "Fraction of keys to skip initially, default:0");
        availableOptions.addOption("t",  "threads",              true,   "Number of threads to use, default:50");
        availableOptions.addOption("c",  "columns",              true,   "Number of columns per key, default:5");
        availableOptions.addOption("S",  "column-size",          true,   "Size of column values in bytes, default:34");
        availableOptions.addOption("C",  "cardinality",          true,   "Number of unique values stored in columns, default:50");
        availableOptions.addOption("d",  "nodes",                true,   "Host nodes (comma separated), default:locahost");
        availableOptions.addOption("D",  "nodesfile",            true,   "File containing host nodes (one per line)");
        availableOptions.addOption("s",  "stdev",                true,   "Standard Deviation Factor, default:0.1");
        availableOptions.addOption("r",  "random",               false,  "Use random key generator (STDEV will have no effect), default:false");
        availableOptions.addOption("f",  "file",                 true,   "Write output to given file");
        availableOptions.addOption("p",  "port",                 true,   "Thrift port, default:9160");
        availableOptions.addOption("m",  "unframed",             false,  "Use unframed transport, default:false");
        availableOptions.addOption("o",  "operation",            true,   "Operation to perform (INSERT, READ, RANGE_SLICE, INDEXED_RANGE_SLICE, MULTI_GET, COUNTER_ADD, COUNTER_GET), default:INSERT");
        availableOptions.addOption("u",  "supercolumns",         true,   "Number of super columns per key, default:1");
        availableOptions.addOption("y",  "family-type",          true,   "Column Family Type (Super, Standard), default:Standard");
        availableOptions.addOption("K",  "keep-trying",          true,   "Retry on-going operation N times (in case of failure). positive integer, default:10");
        availableOptions.addOption("k",  "keep-going",           false,  "Ignore errors inserting or reading (when set, --keep-trying has no effect), default:false");
        availableOptions.addOption("i",  "progress-interval",    true,   "Progress Report Interval (seconds), default:10");
        availableOptions.addOption("g",  "keys-per-call",        true,   "Number of keys to get_range_slices or multiget per call, default:1000");
        availableOptions.addOption("l",  "replication-factor",   true,   "Replication Factor to use when creating needed column families, default:1");
        availableOptions.addOption("L",  "enable-cql",           false,  "Perform queries using CQL (Cassandra Query Language).");
        availableOptions.addOption("e",  "consistency-level",    true,   "Consistency Level to use (ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY), default:ONE");
        availableOptions.addOption("x",  "create-index",         true,   "Type of index to create on needed column families (KEYS)");
        availableOptions.addOption("R",  "replication-strategy", true,   "Replication strategy to use (only on insert if keyspace does not exist), default:org.apache.cassandra.locator.SimpleStrategy");
        availableOptions.addOption("O",  "strategy-properties",  true,   "Replication strategy properties in the following format <dc_name>:<num>,<dc_name>:<num>,...");
        availableOptions.addOption("W",  "no-replicate-on-write",false,  "Set replicate_on_write to false for counters. Only counter add with CL=ONE will work");
        availableOptions.addOption("V",  "average-size-values",  false,  "Generate column values of average rather than specific size");
        availableOptions.addOption("T",  "send-to",              true,   "Send this as a request to the stress daemon at specified address.");
        availableOptions.addOption("I",  "compression",          true,   "Specify the compression to use for sstable, default:no compression");
        availableOptions.addOption("Q",  "query-names",          true,   "Comma-separated list of column names to retrieve from each row.");
        availableOptions.addOption("Z",  "compaction-strategy",  true,   "CompactionStrategy to use.");
        availableOptions.addOption("U",  "comparator",           true,   "Column Comparator to use. Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");

        availableOptions.addOption("A",  "num-dependencies",     true,   "Number of dependencies to attach to each operation.");

        availableOptions.addOption("",  "just-create-keyspace",  false,   "Only create the keyspace and then exit");

        availableOptions.addOption("",  "stress-index",      true,       "Index of this stress client out of STRESS-COUNT.  Allows for disjoint INSERTS on different servers.");
        availableOptions.addOption("",  "stress-count",      true,       "Total number of coordinating stress clients");

        availableOptions.addOption("",  "write-fraction",             true,   "Fraction of ops to be writes, 0-1");
        availableOptions.addOption("",  "columns-per-key-read",       true,   "");
        availableOptions.addOption("",  "columns-per-key-write",      true,   "");
        availableOptions.addOption("",  "keys-per-read",              true,   "");
        availableOptions.addOption("",  "keys-per-write",             true,   "");
        availableOptions.addOption("",  "write-transaction-fraction", true,   "Fraction of ops to be transactions, 0-1");

        availableOptions.addOption("",  "num-servers",             true,   "The number of servers in each cluster, required for write-txn workload");
        availableOptions.addOption("",  "keys-per-server",         true,   "The number of keys to write on each server in a write txn");
        availableOptions.addOption("",  "servers-per-txn",         true,   "The number of servers to include in each write txn");

        availableOptions.addOption("",  "server-index",         true,   "Index of the server (out of num-servers) to load for DYNAMIC_ONE_SERVER");
    }

    private int numKeys          = 1000 * 1000;
    private int numDifferentKeys = numKeys;
    private float skipKeys       = 0;
    private int threads          = 50;
    private int columns          = 5;
    private int columnSize       = 34;
    private int cardinality      = 50;
    private String[] nodes       = new String[] { "127.0.0.1" };
    private boolean random       = false;
    private boolean unframed     = false;
    private int retryTimes       = 10;
    private int port             = 9160;
    private int superColumns     = 1;
    private String compression   = null;
    private String compactionStrategy = null;

    private int progressInterval  = 10;
    private int keysPerCall       = 1000;
    private boolean replicateOnWrite = true;
    private boolean ignoreErrors  = false;
    private boolean enable_cql    = false;

    private final String outFileName;

    private IndexType indexType = null;
    private Stress.Operations operation = Stress.Operations.INSERT;
    private ColumnFamilyType columnFamilyType = ColumnFamilyType.Standard;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private String replicationStrategy = "org.apache.cassandra.locator.SimpleStrategy";
    private final Map<String, String> replicationStrategyOptions = new HashMap<String, String>();

    // if we know exactly column names that we want to read (set by -Q option)
    public final List<ByteBuffer> columnNames;

    public final boolean averageSizeValues;

    // required by Gaussian distribution.
    protected int   mean;
    protected float sigma;

    public final InetAddress sendToDaemon;
    public final String comparator;
    public final boolean timeUUIDComparator;

    //COPS specific microbenchmarking options
    private int numDependencies = 0;
    private final Set<Dep> pregeneratedDependencies = new HashSet<Dep>();

    private int stressIndex = 0;
    private int stressCount = 1;
    private final int keysOffset = 0;

    private boolean justCreateKeyspace = false;

    //COPS dynamic workload generator options
    Map<String, Integer> localServerIPAndPorts = new HashMap<String, Integer>(); //we'll piggyback this off hosts and just use that and assume 9160 for the port
    private double write_fraction = -1;
    //value size already an option
    private int columns_per_key_read = 0;
    private int columns_per_key_write = 0;
    private int keys_per_read = 0;
    private int keys_per_write = 0;
    private double write_transaction_fraction = -1;

    // for write txn experiment where we want to control the exact number of keys being accessed on each server
    private int num_servers = 0;
    private int keys_per_server = 0;
    private int servers_per_txn = 0;

    private int server_index = -1;
    private static ArrayList<ArrayList<ByteBuffer>> generatedKeysByServer;


    public Session(String[] arguments) throws IllegalArgumentException
    {
        float STDev = 0.1f;
        CommandLineParser parser = new PosixParser();

        try
        {
            CommandLine cmd = parser.parse(availableOptions, arguments);

            if (cmd.getArgs().length > 0)
            {
                System.err.println("Application does not allow arbitrary arguments: " + StringUtils.join(cmd.getArgList(), ", "));
                System.exit(1);
            }

            if (cmd.hasOption("h"))
                throw new IllegalArgumentException("help");

            if (cmd.hasOption("n"))
                numKeys = Integer.parseInt(cmd.getOptionValue("n"));

            if (cmd.hasOption("F"))
                numDifferentKeys = Integer.parseInt(cmd.getOptionValue("F"));
            else
                numDifferentKeys = numKeys;

            if (cmd.hasOption("N"))
                skipKeys = Float.parseFloat(cmd.getOptionValue("N"));

            if (cmd.hasOption("t"))
                threads = Integer.parseInt(cmd.getOptionValue("t"));

            if (cmd.hasOption("c"))
                columns = Integer.parseInt(cmd.getOptionValue("c"));

            if (cmd.hasOption("S"))
                columnSize = Integer.parseInt(cmd.getOptionValue("S"));

            if (cmd.hasOption("C"))
                cardinality = Integer.parseInt(cmd.getOptionValue("C"));

            if (cmd.hasOption("d"))
                nodes = cmd.getOptionValue("d").split(",");

            if (cmd.hasOption("D"))
            {
                try
                {
                    String node = null;
                    List<String> tmpNodes = new ArrayList<String>();
                    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(cmd.getOptionValue("D"))));
                    while ((node = in.readLine()) != null)
                    {
                        if (node.length() > 0)
                            tmpNodes.add(node);
                    }
                    nodes = tmpNodes.toArray(new String[tmpNodes.size()]);
                    in.close();
                }
                catch(IOException ioe)
                {
                    throw new RuntimeException(ioe);
                }
            }

            if (cmd.hasOption("s"))
                STDev = Float.parseFloat(cmd.getOptionValue("s"));

            if (cmd.hasOption("r"))
                random = true;

            outFileName = (cmd.hasOption("f")) ? cmd.getOptionValue("f") : null;

            if (cmd.hasOption("p"))
                port = Integer.parseInt(cmd.getOptionValue("p"));

            if (cmd.hasOption("m"))
                unframed = Boolean.parseBoolean(cmd.getOptionValue("m"));

            if (cmd.hasOption("o"))
                operation = Stress.Operations.valueOf(cmd.getOptionValue("o").toUpperCase());

            if (cmd.hasOption("u"))
                superColumns = Integer.parseInt(cmd.getOptionValue("u"));

            if (cmd.hasOption("y"))
                columnFamilyType = ColumnFamilyType.valueOf(cmd.getOptionValue("y"));

            if (cmd.hasOption("K"))
            {
                retryTimes = Integer.valueOf(cmd.getOptionValue("K"));

                if (retryTimes <= 0)
                {
                    throw new RuntimeException("--keep-trying option value should be > 0");
                }
            }

            if (cmd.hasOption("k"))
            {
                retryTimes = 1;
                ignoreErrors = true;
            }


            if (cmd.hasOption("i"))
                progressInterval = Integer.parseInt(cmd.getOptionValue("i"));

            if (cmd.hasOption("g"))
                keysPerCall = Integer.parseInt(cmd.getOptionValue("g"));

            if (cmd.hasOption("e"))
                consistencyLevel = ConsistencyLevel.valueOf(cmd.getOptionValue("e").toUpperCase());

            if (cmd.hasOption("x"))
                indexType = IndexType.valueOf(cmd.getOptionValue("x").toUpperCase());

            if (cmd.hasOption("R"))
                replicationStrategy = cmd.getOptionValue("R");

            if (cmd.hasOption("l"))
                replicationStrategyOptions.put("replication_factor", String.valueOf(Integer.parseInt(cmd.getOptionValue("l"))));
            else if (replicationStrategy.endsWith("SimpleStrategy"))
                replicationStrategyOptions.put("replication_factor", "1");

            if (cmd.hasOption("L"))
                enable_cql = true;

            if (cmd.hasOption("O"))
            {
                String[] pairs = StringUtils.split(cmd.getOptionValue("O"), ',');

                for (String pair : pairs)
                {
                    String[] keyAndValue = StringUtils.split(pair, ':');

                    if (keyAndValue.length != 2)
                        throw new RuntimeException("Invalid --strategy-properties value.");

                    replicationStrategyOptions.put(keyAndValue[0], keyAndValue[1]);
                }
            }

            if (cmd.hasOption("W"))
                replicateOnWrite = false;

            if (cmd.hasOption("I"))
                compression = cmd.getOptionValue("I");

            averageSizeValues = cmd.hasOption("V");

            try
            {
                sendToDaemon = cmd.hasOption("send-to")
                                ? InetAddress.getByName(cmd.getOptionValue("send-to"))
                                : null;
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }

            if (cmd.hasOption("Q"))
            {
                AbstractType comparator = TypeParser.parse(DEFAULT_COMPARATOR);

                String[] names = StringUtils.split(cmd.getOptionValue("Q"), ",");
                columnNames = new ArrayList<ByteBuffer>(names.length);

                for (String columnName : names)
                    columnNames.add(comparator.fromString(columnName));
            }
            else
            {
                columnNames = null;
            }

            if (cmd.hasOption("Z"))
            {
                compactionStrategy = cmd.getOptionValue("Z");

                try
                {
                    // validate compaction strategy class
                    CFMetaData.createCompactionStrategy(compactionStrategy);
                }
                catch (ConfigurationException e)
                {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }
            }

            if (cmd.hasOption("U"))
            {
                AbstractType parsed = null;

                try
                {
                    parsed = TypeParser.parse(cmd.getOptionValue("U"));
                }
                catch (ConfigurationException e)
                {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }

                comparator = cmd.getOptionValue("U");
                timeUUIDComparator = parsed instanceof TimeUUIDType;

                if (!(parsed instanceof TimeUUIDType || parsed instanceof AsciiType || parsed instanceof UTF8Type))
                {
                    System.err.println("Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
                    System.exit(1);
                }
            }
            else
            {
                comparator = null;
                timeUUIDComparator = false;
            }

            //num-dependencies
            if (cmd.hasOption("A"))
            {
                numDependencies = Integer.parseInt(cmd.getOptionValue("A"));
                for (int i = 0; i < numDependencies; ++i) {
                    //we'll just include dummy dependencies, this is for microbenchmarks so they shouldn't be checked
                    ByteBuffer locator_key = ByteBufferUtil.bytes(String.valueOf(i));
                    long timestamp = i;
                    pregeneratedDependencies.add(new Dep(locator_key, timestamp));
                }
            }

            if (cmd.hasOption("stress-index")) {
                stressIndex = Integer.parseInt(cmd.getOptionValue("stress-index"));
                if (stressIndex < 0)
                    throw new RuntimeException("Invalid --stress-index value");
            }
            if (cmd.hasOption("stress-count")) {
                stressCount = Integer.parseInt(cmd.getOptionValue("stress-count"));
                if (stressCount <= 0)
                    throw new RuntimeException("Invalid --stress-count value");
            }

            if (cmd.hasOption("just-create-keyspace")) {
                justCreateKeyspace = true;
            }

            if (cmd.hasOption("write-fraction")) {
                write_fraction = Double.parseDouble(cmd.getOptionValue("write-fraction"));
                if (write_fraction < 0 || write_fraction > 1)
                    throw new RuntimeException("Invalid --write-fraction value");
            }
            if (cmd.hasOption("columns-per-key-read")) {
                columns_per_key_read = Integer.parseInt(cmd.getOptionValue("columns-per-key-read"));
                if (columns_per_key_read <= 0)
                    throw new RuntimeException("Invalid columns-per-key-read value");
            }
            if (cmd.hasOption("columns-per-key-write")) {
                columns_per_key_write = Integer.parseInt(cmd.getOptionValue("columns-per-key-write"));
                if (columns_per_key_write <= 0)
                    throw new RuntimeException("Invalid columns-per-key-write value");
            }
            if (cmd.hasOption("keys-per-read")) {
                keys_per_read = Integer.parseInt(cmd.getOptionValue("keys-per-read"));
                if (keys_per_read <= 0)
                    throw new RuntimeException("Invalid keys-per-read value");
            }
            if (cmd.hasOption("keys-per-write")) {
                keys_per_write = Integer.parseInt(cmd.getOptionValue("keys-per-write"));
                if (keys_per_write <= 0)
                    throw new RuntimeException("Invalid keys-per-write value");
            }
            if (cmd.hasOption("write-transaction-fraction")) {
                write_transaction_fraction = Double.parseDouble(cmd.getOptionValue("write-transaction-fraction"));
                if (write_transaction_fraction < 0 || write_transaction_fraction > 1)
                    throw new RuntimeException("Invalid --write-transaction-fraction value");
            }

            if (cmd.hasOption("num-servers")) {
                num_servers = Integer.parseInt(cmd.getOptionValue("num-servers"));
                if (num_servers <= 0)
                    throw new RuntimeException("Invalid num-servers value");
            }
            if (cmd.hasOption("keys-per-server")) {
                keys_per_server = Integer.parseInt(cmd.getOptionValue("keys-per-server"));
                if (keys_per_server <= 0)
                    throw new RuntimeException("Invalid key-per-server value");
            }
            if (cmd.hasOption("servers-per-txn")) {
                servers_per_txn = Integer.parseInt(cmd.getOptionValue("servers-per-txn"));
                if (servers_per_txn <= 0)
                    throw new RuntimeException("Invalid servers-per-txn value");
            }
            if (cmd.hasOption("server-index")) {
                server_index = Integer.parseInt(cmd.getOptionValue("server-index"));
                if (server_index < 0)
                    throw new RuntimeException("Invalid server-index value");
            }
        }
        catch (ParseException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        catch (ConfigurationException e)
        {
            throw new IllegalStateException(e.getMessage(), e);
        }

        if (operation == Stress.Operations.DYNAMIC || operation == Stress.Operations.DYNAMIC_ONE_SERVER) {
            //Must set all dynamic workload parameters or none
            if (! (write_fraction >= 0 && columns_per_key_read != 0 && columns_per_key_write != 0
                    && keys_per_read != 0 && keys_per_write != 0 && write_transaction_fraction >= 0
                    && columnSize != 34)) {
                throw new RuntimeException("All dynamic options must be set");
            }

            if (operation == Stress.Operations.DYNAMIC_ONE_SERVER) {
                if (num_servers == 0 || server_index == -1) {
                    throw new RuntimeException("Dynamic One Server requires num-servers, and server-index");
                }
                //DYNAMIC_ONE_SERVER should get a numDifferentKeys==totalKeys written in the system, just like normal dynamic...
                dynamicOneServerGenerateKeysForEachServer(num_servers, numDifferentKeys);
            }
        }

        if (operation == Stress.Operations.WRITE_TXN || operation == Stress.Operations.BATCH_MUTATE) {
            if (num_servers == 0 || keys_per_server == 0 || servers_per_txn == 0 || numKeys == 0 || columns_per_key_write == 0) {
                throw new RuntimeException("Write txn required num-servers, keys-per-server, servers-per-txn, columns-per-key-write, num-keys, and num-different-keys options");
            }
            assert servers_per_txn <= num_servers;
            generateKeysForEachServer(num_servers, numDifferentKeys);
        }

        for (String node : nodes) {
            localServerIPAndPorts.put(node, 9160);
        }

        if (justCreateKeyspace) {
            this.createKeySpaces();
            System.exit(0);
        }


        mean  = numDifferentKeys / 2;
        sigma = numDifferentKeys * STDev;

        operations = new AtomicInteger();
        keys = new AtomicInteger();
        columnCount = new AtomicInteger();
        bytes = new AtomicLong();
        latency = new AtomicLong();
        latencies = new ConcurrentLinkedQueue<Long>();
    }

    public int getCardinality()
    {
        return cardinality;
    }

    public int getColumnSize()
    {
        return columnSize;
    }

    public boolean isUnframed()
    {
        return unframed;
    }

    public int getColumnsPerKey()
    {
        return columns;
    }

    public ColumnFamilyType getColumnFamilyType()
    {
        return columnFamilyType;
    }

    public int getNumKeys()
    {
        return numKeys;
    }

    public int getNumDifferentKeys()
    {
        return numDifferentKeys;
    }

    public int getKeysOffset()
    {
        return numDifferentKeys*stressIndex;
    }

    public int getThreads()
    {
        return threads;
    }

    public float getSkipKeys()
    {
        return skipKeys;
    }

    public int getSuperColumns()
    {
        return superColumns;
    }

    public int getKeysPerThread()
    {
        return numKeys / threads;
    }

    public int getTotalKeysLength()
    {
        //return Integer.toString(numDifferentKeys*stressCount).length();
	return 10;
    }

    public int getNumTotalKeys()
    {
        return numDifferentKeys*stressCount;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    public int getRetryTimes()
    {
        return retryTimes;
    }

    public boolean ignoreErrors()
    {
        return ignoreErrors;
    }

    public Stress.Operations getOperation()
    {
        return operation;
    }

    public PrintStream getOutputStream()
    {
        try
        {
            return (outFileName == null) ? System.out : new PrintStream(new FileOutputStream(outFileName));
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public int getProgressInterval()
    {
        return progressInterval;
    }

    public boolean useRandomGenerator()
    {
        return random;
    }

    public int getKeysPerCall()
    {
        return keysPerCall;
    }

    // required by Gaussian distribution
    public int getMean()
    {
        return mean;
    }

    // required by Gaussian distribution
    public float getSigma()
    {
        return sigma;
    }

    public boolean isCQL()
    {
        return enable_cql;
    }

    public Set<Dep> getPregeneratedDependencies()
    {
        return pregeneratedDependencies;
    }

    public double getWrite_fraction()
    {
        return write_fraction;
    }

    public int getColumns_per_key_read()
    {
        return columns_per_key_read;
    }

    public int getColumns_per_key_write()
    {
        return columns_per_key_write;
    }

    public int getKeys_per_read()
    {
        return keys_per_read;
    }

    public int getKeys_per_write()
    {
        return keys_per_write;
    }

    public double getWrite_transaction_fraction()
    {
        return write_transaction_fraction;
    }

    public int getNum_servers()
    {
        return num_servers;
    }

    public int getKeys_per_server()
    {
        return keys_per_server;
    }

    public int getServers_per_txn()
    {
        return servers_per_txn;
    }

    public int getServerIndex()
    {
        assert server_index != -1;
        return server_index;
    }

    /**
     * Create Keyspace1 with Standard1 and Super1 column families
     */
    public void createKeySpaces()
    {
        KsDef keyspace = new KsDef();
        String defaultComparator = comparator == null ? DEFAULT_COMPARATOR : comparator;

        // column family for standard columns
        CfDef standardCfDef = new CfDef("Keyspace1", "Standard1");
        Map<String, String> compressionOptions = new HashMap<String, String>();
        if (compression != null)
            compressionOptions.put("sstable_compression", compression);

        standardCfDef.setComparator_type(defaultComparator)
                     .setDefault_validation_class(DEFAULT_VALIDATOR)
                     .setCompression_options(compressionOptions);

        standardCfDef.setCaching("all");

        standardCfDef.setRead_repair_chance(0);

        if (indexType != null)
        {
            ColumnDef standardColumn = new ColumnDef(ByteBufferUtil.bytes("C1"), "BytesType");
            standardColumn.setIndex_type(indexType).setIndex_name("Idx1");
            standardCfDef.setColumn_metadata(Arrays.asList(standardColumn));
        }

        // column family with super columns
        CfDef superCfDef = new CfDef("Keyspace1", "Super1").setColumn_type("Super");
        superCfDef.setComparator_type(DEFAULT_COMPARATOR)
                  .setSubcomparator_type(defaultComparator)
                  .setDefault_validation_class(DEFAULT_VALIDATOR)
                  .setCompression_options(compressionOptions)
                  .setRead_repair_chance(0);

        // column family for standard counters
        CfDef counterCfDef = new CfDef("Keyspace1", "Counter1");
        counterCfDef.setDefault_validation_class("CounterColumnType")
                    .setReplicate_on_write(replicateOnWrite)
                    .setCompression_options(compressionOptions)
                    .setRead_repair_chance(0);

        // column family with counter super columns
        CfDef counterSuperCfDef = new CfDef("Keyspace1", "SuperCounter1");
        counterSuperCfDef.setDefault_validation_class("CounterColumnType")
                         .setReplicate_on_write(replicateOnWrite)
                         .setColumn_type("Super")
                         .setCompression_options(compressionOptions)
                         .setRead_repair_chance(0);

        keyspace.setName("Keyspace1");
        keyspace.setStrategy_class(replicationStrategy);

        if (!replicationStrategyOptions.isEmpty())
        {
            keyspace.setStrategy_options(replicationStrategyOptions);
        }

        if (compactionStrategy != null)
        {
            standardCfDef.setCompaction_strategy(compactionStrategy);
            superCfDef.setCompaction_strategy(compactionStrategy);
            counterCfDef.setCompaction_strategy(compactionStrategy);
            counterSuperCfDef.setCompaction_strategy(compactionStrategy);
        }

        keyspace.setCf_defs(new ArrayList<CfDef>(Arrays.asList(standardCfDef, superCfDef, counterCfDef, counterSuperCfDef)));

        Cassandra.Client client = getClient(false);

        try
        {
            client.system_add_keyspace(keyspace);
	    int sleepTime = 5;
            System.out.println(String.format("Created keyspaces. Sleeping %ss for propagation.", sleepTime));
            Thread.sleep(sleepTime * 1000); // seconds
        }
        catch (InvalidRequestException e)
        {
            System.err.println("Unable to create stress keyspace: " + e.getWhy());
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
    }

    public ClientLibrary getClientLibrary()
    {
	// Allow use of client library with other consistency levels for micro-benchmarks
        //if (this.getConsistencyLevel() != ConsistencyLevel.LOCAL_QUORUM) {
        //    throw new RuntimeException("Session.getClientLibrary is only meant for use with consistency level LOCAL_QUORUM");
        //}

        try {
            return new ClientLibrary(localServerIPAndPorts, "Keyspace1", this.getConsistencyLevel());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }


    public ByteBuffer getRandGeneratedKey(int serverNum)
    {
        int serverKeyCount = generatedKeysByServer.get(serverNum).size();
        return generatedKeysByServer.get(serverNum).get(Stress.randomizer.nextInt(serverKeyCount));
    }

    private void generateKeysForEachServer(int numServers, int totalNumKeys)
    {
        int keysPerServer = totalNumKeys/numServers;

        generatedKeysByServer = new ArrayList<ArrayList<ByteBuffer>>(numServers);
        for (int i = 0; i < numServers; i++)
        {
            generatedKeysByServer.add(new ArrayList<ByteBuffer>(keysPerServer));
        }

        // Assuming we're using the random partitioner, which we are.
        // We need to generate keys for servers by randomly generating keys
        // and then matching them to whatever their md5 maps to.
        boolean allServersFull;
        Random randomizer = new Random();
        do {
            String randKeyStr = String.format("%0" + (getTotalKeysLength()) + "d", randomizer.nextInt(10*(getNumDifferentKeys() - 1)));
            ByteBuffer randKey = ByteBuffer.wrap(randKeyStr.getBytes(UTF_8));
            double hashedRandKey = FBUtilities.hashToBigInteger(randKey).doubleValue();

            //Cassandra's keyspace is [0, 2**127)
            double keyrangeSize = Math.pow(2, 127) / numServers;
            int serverIndex = (int) (hashedRandKey / keyrangeSize);
            if (generatedKeysByServer.get(serverIndex).size() < keysPerServer) {
                generatedKeysByServer.get(serverIndex).add(randKey);
            }

            allServersFull = true;
            for (int i = 0; i < numServers; i++) {
                //System.out.println("Server " + i + " has " + generatedKeysByServer.get(i).size() + " keys");
                if (generatedKeysByServer.get(i).size() < keysPerServer) {
                    allServersFull = false;
                    break;
                }
            }
        } while (!allServersFull);
    }

    private void dynamicOneServerGenerateKeysForEachServer(int numServers, int numPopulatedKeys)
    {
        generatedKeysByServer = new ArrayList<ArrayList<ByteBuffer>>(numServers);
        for (int i = 0; i < numServers; i++)
        {
            generatedKeysByServer.add(new ArrayList<ByteBuffer>());
        }

        // Assuming we're using the random partitioner, which we are.
        // We need to generate keys for servers by randomly generating keys
        // and then matching them to whatever their md5 maps to.
        for (int keyI = 0; keyI < numPopulatedKeys; keyI++)
        {
            String keyStr = String.format("%0" + (getTotalKeysLength()) + "d", keyI);
            ByteBuffer key = ByteBuffer.wrap(keyStr.getBytes(UTF_8));
            double hashedKey = FBUtilities.hashToBigInteger(key).doubleValue();

            //Cassandra's keyspace is [0, 2**127)
            double keyrangeSize = Math.pow(2, 127) / numServers;
            int serverIndex = (int) (hashedKey / keyrangeSize);
            generatedKeysByServer.get(serverIndex).add(key);
        }
    }



    /**
     * Thrift client connection with Keyspace1 set.
     * @return cassandra client connection
     */
    public Cassandra.Client getClient()
    {
        return getClient(true);
    }
    /**
     * Thrift client connection
     * @param setKeyspace - should we set keyspace for client or not
     * @return cassandra client connection
     */
    public Cassandra.Client getClient(boolean setKeyspace)
    {
        // random node selection for fake load balancing
        String currentNode = nodes[Stress.randomizer.nextInt(nodes.length)];

        TSocket socket = new TSocket(currentNode, port);
        TTransport transport = (isUnframed()) ? socket : new TFramedTransport(socket);
        Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(transport));

        try
        {
            transport.open();

            if (setKeyspace)
            {
                client.set_keyspace("Keyspace1", LamportClock.sendTimestamp());
            }
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }

        return client;
    }

    public static InetAddress getLocalAddress()
    {
        if (localInetAddress == null)
        {
            try
            {
                localInetAddress = InetAddress.getLocalHost();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }

        return localInetAddress;
    }
}
