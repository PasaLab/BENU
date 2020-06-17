package cn.edu.nju.pasalab.db.hbase;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import cn.edu.nju.pasalab.db.util.CoderAndDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by wangzhaokang on 2/6/18.
 */
public final class HBaseOperation {

    public static final String CONF_DEFAULT_NUM_REGIONS = "hbase.default.num.regions"; // integer
    public static final String DEFAULT_DEFAULT_NUM_REGIONS ="16";
    public static final String CONF_ZOOKEEPER_QUORUM  = "hbase.zookeeper.quorum"; // separated by comma
    public static final String DEFAULT_ZOOKEEPER_QUORUM = "localhost";

    private int defaultNumRegions;
    private String hbaseZookeeperQuorum;
    private Configuration hConf;
    private Admin admin;
    private Connection connection;

    public HBaseOperation(Properties hbaseConf) throws IOException {
        loadConfigurations(hbaseConf);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        conf.set("hbase.client.keyvalue.maxsize", "0"); // disable the maxsize check
        hConf = conf;
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    private void loadConfigurations(Properties conf) {
        String numRegionsString = conf.getProperty(CONF_DEFAULT_NUM_REGIONS, DEFAULT_DEFAULT_NUM_REGIONS);
        defaultNumRegions = Integer.parseInt(numRegionsString);
        String zookeeperString = conf.getProperty(CONF_ZOOKEEPER_QUORUM, DEFAULT_ZOOKEEPER_QUORUM);
        this.hbaseZookeeperQuorum = zookeeperString;
    }


    public Admin getAdmin() {
        return admin;
    }

    public Configuration gethConf() {
        return hConf;
    }

    public Connection getConnection() {
        return connection;
    }

    public void close() throws IOException {
        admin.close();
        connection.close();
    }

    public void createTable(String tableName, String columnFamily) throws IOException {
        createTable(tableName, Bytes.toBytes(columnFamily), defaultNumRegions + 2);
    }

    public void createTable(String tableName, byte columnFamily[], int splitNum) throws IOException {
        byte start[] = CoderAndDecoder.toByteArray(0L), end[] = CoderAndDecoder.toByteArray(~0L);
        // Ranges from all 64 bit space
        createTable(tableName, columnFamily, start, end, splitNum + 2);
    }

    public void createTable(String tableName, byte columnFamily[], byte start[], byte end[], int splitNum)
            throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.setCompactionEnabled(true);
        tableDescriptor.setMemStoreFlushSize(512 << 20);
        HColumnDescriptor cfDescriptor = new HColumnDescriptor(columnFamily);
        tableDescriptor.addFamily(cfDescriptor);
        admin.createTable(tableDescriptor, start, end, splitNum);
    }

    public void deleteTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    }

    public static int compareByteArrayInHBaseWay(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    public static void main(String args[]) throws Exception {
        Properties conf = new Properties();
        String confFile = args[0];
        InputStream inputStream = new FileInputStream(confFile);
        conf.load(inputStream);
        inputStream.close();
        HBaseOperation operation = new HBaseOperation(conf);
        operation.deleteTable("dblp");
        operation.createTable("dblp", Bytes.toBytes("record"), 64);
        operation.close();
    }
}