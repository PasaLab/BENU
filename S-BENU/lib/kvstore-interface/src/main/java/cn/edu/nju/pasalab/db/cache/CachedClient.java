package cn.edu.nju.pasalab.db.cache;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import org.caffinitas.ohc.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Created by wzk on 18-4-28.
 */
public class CachedClient extends BasicKVDatabaseClient {

    static public final String CONF_CACHE_CAPACITY = "cache.capacity.in.byte"; // in byte
    static public final String DEFAULT_CACHE_CAPACITY = "8,388,608"; // 8 MB by default
    static public final String CONF_CACHE_STATS_FILE_PATH = "cache.stats.file.path"; // store cache stats
    static public final String DEFAULT_CACHE_STATS_FILE_PATH = "/tmp/cache.stats";
    static public final String CONF_CACHE_CONCURRENCY = "cache.concurrency";
    static public final String DEFAULT_CACHE_CONCURRENCY = "4";
    static public final String CONF_CACHE_HASHTABLE_SIZE_PER_SEGMENT = "cache.hashtable.size.per.segment";
    static public final String DEFAULT_CACHE_HASHTABLE_SIZE_PER_SEGMENT = "8192";
    static public final String CONF_DB_BACKEND_CLASS_NAME = "cache.db.backend.class.name"; // required, no default!

    private static Thread cacheStatsReportThread = null;


    private BasicKVDatabaseClient db;
    private OHCache<byte[], byte[]> cache;
    private long cacheCapacityInBytes = 1;
    private String statsFilePath;
    private int concurrency; // = segment_count in the OHC cache.
    private int hashTableSizePerSegment;
    private String dbClassName;
    private AtomicLong queryCount = new AtomicLong(0L);
    private AtomicLong hitCount = new AtomicLong(0L);
    private Logger logger = Logger.getLogger(this.getClass().getName());


    /**
     * @param key
     * @return the value. Return null if the key does not exist.
     * @throws Exception
     */
    @Override
    public byte[] get(byte[] key) throws Exception {
        assert cache != null;
        this.queryCount.getAndAdd(1L);
        byte[] result = cache.get(key);
        if (result == null) {
            result = db.get(key);
            if (result != null) {
                cache.put(key, result);
            }
        } else {
            this.hitCount.getAndAdd(1L);
        }
        return result;
    }

    @Override
    public byte[][] getAll(byte keys[][]) throws Exception{
        byte[][] results = new byte[keys.length][];
        IntArrayList queryKeysIDs = new IntArrayList();
        long localQueryCount = 0L;
        long localHitCount = 0L;

        for (int i = 0; i < keys.length; i++) {
            localQueryCount++;
            byte[] result = cache.get(keys[i]);
            if (result == null) {
                queryKeysIDs.add(i);
            } else {
                results[i] = result;
                localHitCount++;
            }
        }
        this.queryCount.addAndGet(localQueryCount);
        this.hitCount.addAndGet(localHitCount);
        byte[][] queryKeys = new byte[queryKeysIDs.size()][];
        for (int i = 0; i < queryKeysIDs.size(); i++) {
            queryKeys[i] = keys[queryKeysIDs.getInt(i)];
        }
        byte[][] queryResults = db.getAll(queryKeys);
        for (int i = 0; i < queryKeysIDs.size(); i++) {
            int kID = queryKeysIDs.getInt(i);
            results[kID] = queryResults[i];
            if (queryResults[i] != null) {
                cache.put(queryKeys[i], queryResults[i]);
            }

        }
        return results;
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        db.put(key, value);
    }

    @Override
    public void putAll(byte[][] keys, byte[][] values) throws Exception {
        db.putAll(keys, values);
    }

    /**
     * Close the database connections.
     */
    @Override
    public void close() throws Exception {
        cache.clear();
        cache.close();
        db.close();
        cacheStatsReportThread.interrupt();
    }

    /**
     * Clear the contents in the database
     */
    @Override
    public void clearDB() throws Exception {
        cache.clear();
        db.clearDB();
    }

    @Override
    public void createDB() throws Exception {
        db.createDB();
    }

    private void loadConfigurations(Properties conf) throws IOException {
        String capacityString = conf.getProperty(CONF_CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY);
        this.cacheCapacityInBytes = Long.valueOf(capacityString);
        this.statsFilePath = conf.getProperty(CONF_CACHE_STATS_FILE_PATH, DEFAULT_CACHE_STATS_FILE_PATH);
        this.concurrency = Integer.parseInt(conf.getProperty(CONF_CACHE_CONCURRENCY, DEFAULT_CACHE_CONCURRENCY));
        this.hashTableSizePerSegment = Integer.parseInt(
                conf.getProperty(CONF_CACHE_HASHTABLE_SIZE_PER_SEGMENT,
                        DEFAULT_CACHE_HASHTABLE_SIZE_PER_SEGMENT));
        this.dbClassName = conf.getProperty(CONF_DB_BACKEND_CLASS_NAME);
        long totalHashTableSize = hashTableSizePerSegment * concurrency * 8; // according to the equation
        String configurationInfo = String.format("Get configurations: %s.\n"
                + "Set cache capacity: %d bytes.\n"
                + "# Segments: %d.\n"
                + "Total hash table size: %d bytes.",
                conf, this.cacheCapacityInBytes, this.concurrency, totalHashTableSize);
        logger.info(configurationInfo);
    }

    /**
     * Connect to the underlying database
     *
     * @param conf Database connection-related configurations
     * @throws Exception database connection exception
     */
    @Override
    public void connect(Properties conf) throws Exception {
        loadConfigurations(conf);
        // Connect database
        logger.info("Database backend:" + dbClassName);
        Class dbClass = Class.forName(dbClassName);
        this.db = (BasicKVDatabaseClient) dbClass.newInstance();
        db.connect(conf);
        // create the cache
        OHCacheBuilder<byte[], byte[]> builder = OHCacheBuilder.<byte[], byte[]>newBuilder()
                .keySerializer(new ByteArraySerializer())
                .valueSerializer(new ByteArraySerializer())
                .segmentCount(concurrency)
                .hashTableSize(hashTableSizePerSegment)
                .capacity(cacheCapacityInBytes)
                .throwOOME(true);
        this.cache = builder.build();
        cacheStatsReportThread = new Thread(new CacheStatsReportRunnable(2));
        cacheStatsReportThread.setDaemon(true);
        cacheStatsReportThread.setName("Cache Stats Reporter");
        cacheStatsReportThread.start();

    }

    private class ByteArraySerializer implements CacheSerializer<byte[]> {

        @Override
        public void serialize(byte[] bytes, ByteBuffer byteBuffer) {
            byteBuffer.put(bytes);
        }

        @Override
        public byte[] deserialize(ByteBuffer byteBuffer) {
            byte[] array = new byte[byteBuffer.remaining()];
            byteBuffer.get(array, 0, array.length);
            return array;
        }

        @Override
        public int serializedSize(byte[] bytes) {
            return bytes.length;
        }
    }


    private class CacheStatsReportRunnable implements Runnable {

        private long sleepTimeInSecond = 2;

        public CacheStatsReportRunnable() {
        }

        public CacheStatsReportRunnable(long sleepTimeInSecond) {
            this.sleepTimeInSecond = sleepTimeInSecond;
        }

        private void writeStatsToFile() {
            try {
                PrintWriter writer = new PrintWriter(new File(statsFilePath));
                long missCount = queryCount.get() - hitCount.get();
                writer.println(String.format("CacheStats{hitCount=%d, missCount=%d, }", hitCount.get(), missCount));
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }


        @Override
        public void run() {
            while (true) {
                writeStatsToFile();
                try {
                    Thread.sleep(sleepTimeInSecond * 1000, 0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
