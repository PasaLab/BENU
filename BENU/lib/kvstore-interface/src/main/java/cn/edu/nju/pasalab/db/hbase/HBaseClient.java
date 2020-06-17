package cn.edu.nju.pasalab.db.hbase;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by wzk on 18-4-28.
 */
public final class HBaseClient extends BasicKVDatabaseClient {

    public static final String CONF_TABLE_NAME = "hbase.table.name";
    public static final String DEFAULT_TABLE_NAME = "datatable";
    public static final String CONF_COLUMN_FAMILY = "hbase.column.family";
    public static final String DEFAULT_COLUMN_FAMILY = "data";
    public static final String CONF_COLUMN_NAME = "hbase.column.name";
    public static final String DEFAULT_COLUMN_NAME = "content";
    public static final String CONF_NUM_REGION = "hbase.num.region";
    public static final String DEFAULT_NUM_REGION = "16";
    public static final String CONF_ZOOKEEPER_QUORUM  = "hbase.zookeeper.quorum"; // separated by comma
    public static final String DEFAULT_ZOOKEEPER_QUORUM = "localhost";
    public static final String CONF_USE_HASHED_KEY = "hbase.use.hashed.key";
    public static final String DEFAULT_USE_HASHED_KEY = "false";



    private Configuration hadoopConf;
    private String dataTableNameString;
    private TableName dataTableName;
    private byte[] columnFamily;
    private byte[] columnName;
    private int numRegion;
    private String zookeeperQuorum;
    private boolean useHashedKey;
    private HBaseOperation hBaseOperation;
    private Connection hbaseConnection;

    private Logger logger = Logger.getLogger(this.getClass().getName());

    private boolean isConnectionEstablished() {
        return hbaseConnection != null;
    }

    private byte[] transformKey(byte[] key) {
        if (!useHashedKey) {
            return key;
        } else {
            // Append a byte of hash value to the head of the key to get a new key
            // New key: [ HashValue(1 byte) | original key ]
            byte hashValue = (byte) Arrays.hashCode(key);
            byte[] newHashKey = new byte[key.length + 1];
            newHashKey[0] = hashValue;
            for (int i = 0; i < key.length; i++) {
                newHashKey[i + 1] = key[i];
            }
            return newHashKey;
        }
    }

    /**
     * @param key
     * @return the value. Return null if the key does not exist.
     * @throws Exception
     */
    @Override
    public byte[] get(byte[] key) throws Exception {
        assert isConnectionEstablished();
        byte[] actualKey = transformKey(key);
        Table table = hbaseConnection.getTable(dataTableName);
        Get get = new Get(actualKey);
        get.addColumn(this.columnFamily, this.columnName);
        Result result = table.get(get);
        byte rawResult[] = result.getValue(this.columnFamily, this.columnName);
        table.close();
        return rawResult;
    }

    /**
     * Get the values of a group of keys
     * @param keys
     * @return the values of the keys
     * @throws Exception
     */
    @Override
    public byte[][] getAll(byte keys[][]) throws Exception{
        byte[][] results = new byte[keys.length][];
        Table table = hbaseConnection.getTable(this.dataTableName);
        ArrayList<Get> gets = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            byte[] actualKey = transformKey(keys[i]);
            Get get = new Get(actualKey);
            gets.add(get);
        }
        Result[] hbaseResults = table.get(gets);
        for (int i = 0; i < hbaseResults.length; i++) {
            Result result = hbaseResults[i];
            byte[] value = result.getValue(this.columnFamily, this.columnName);
            results[i] = value;
        }
        table.close();
        return results;
    }


    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        assert isConnectionEstablished();
        Table table = hbaseConnection.getTable(dataTableName);
        byte[] actualKey = transformKey(key);
        Put put = new Put(actualKey);
        put.addColumn(this.columnFamily,this.columnName, value);
        table.put(put);
        table.close();
    }

    /**
     * Set the key-value pairs in batch
     * @param keys
     * @param values
     * @throws Exception
     */
    @Override
    public void putAll(byte keys[][], byte values[][]) throws Exception {
        BufferedMutator mutator = hbaseConnection.getBufferedMutator(dataTableName);
        for (int i = 0; i < keys.length; i++) {
            byte[] actualKey = transformKey(keys[i]);
            Put put = new Put(actualKey);
            put.addColumn(this.columnFamily, this.columnName, values[i]);
            mutator.mutate(put);
        }
        mutator.close();
    }

    /**
     * Close the database connections.
     */
    @Override
    public void close() throws Exception {
        hbaseConnection.close();
        hBaseOperation.close();
    }

    private void loadConfiguration(Properties conf) {
        logger.info("Get configurations:" + conf);
        String tableNameString = conf.getProperty(CONF_TABLE_NAME, DEFAULT_TABLE_NAME);
        this.dataTableNameString = tableNameString;
        String columnFamilyString = conf.getProperty(CONF_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY);
        this.columnFamily = Bytes.toBytes(columnFamilyString);
        String columnNameString = conf.getProperty(CONF_COLUMN_NAME, DEFAULT_COLUMN_NAME);
        this.columnName = Bytes.toBytes(columnNameString);
        String numRegionString = conf.getProperty(CONF_NUM_REGION, DEFAULT_NUM_REGION);
        this.numRegion = Integer.parseInt(numRegionString);
        String useHashedKeyString = conf.getProperty(CONF_USE_HASHED_KEY, DEFAULT_USE_HASHED_KEY);
        this.useHashedKey = Boolean.parseBoolean(useHashedKeyString);
        conf.setProperty(CONF_NUM_REGION, Integer.toString(this.numRegion)); // further used by HBaseOperation
        this.zookeeperQuorum = conf.getProperty(CONF_ZOOKEEPER_QUORUM, DEFAULT_ZOOKEEPER_QUORUM);
        conf.setProperty(CONF_ZOOKEEPER_QUORUM, this.zookeeperQuorum); // further used by HBaseOperation
    }

    /**
     * Connect to the underlying database
     *
     * @param conf Database connection-related configurations
     * @throws Exception database connection exception
     */
    @Override
    public void connect(Properties conf) throws Exception {
        loadConfiguration(conf);
        hBaseOperation = new HBaseOperation(conf);
        hadoopConf = hBaseOperation.gethConf();
        hbaseConnection = ConnectionFactory.createConnection(hadoopConf);
        dataTableName = TableName.valueOf(this.dataTableNameString);
        logger.info("HBase connection established.");
    }

    /**
     * Clear the contents in the database
     */
    @Override
    public void clearDB() throws Exception {
        hBaseOperation.deleteTable(this.dataTableNameString);
        hBaseOperation.createTable(this.dataTableNameString, this.columnFamily, numRegion);
    }

    @Override
    public void createDB() throws Exception {
        hBaseOperation.createTable(this.dataTableNameString, this.columnFamily, numRegion);
    }
}
