package cn.edu.nju.pasa.graph.storage;

import cn.edu.nju.pasa.graph.conf.ProcessLevelConfReader;
import cn.edu.nju.pasa.graph.storage.hbase.HBaseGraphStorage;
import com.carrotsearch.sizeof.RamUsageEstimator;
import com.github.benmanes.caffeine.cache.*;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;

/**
 * A simple graph storage based on client-sharding Redis simple graph storage.
 * This storage is enhanced with a local cache.
 * Created by wangzhaokang on 10/24/17.
 */
public class CacheBasedDistributedGraphStorage extends AbstractSimpleGraphStorage {

    private AbstractSimpleGraphStorage distributedGraphStorage;
    private LoadingCache<Integer, int[]> graphCache;
    //public Cache<Long, int[]> triangleCache;
    private double cacheCapacityInGB = 1;

    private static CacheBasedDistributedGraphStorage processLevelStorage = null;
    private static boolean isInited = false;
    private static boolean processLevelCacheCreateError = false;
    private static Thread cacheStatsReportThread = null;

    private static synchronized void initProcessLevelCache() throws Exception {
        if (!isInited) {
            processLevelStorage = new CacheBasedDistributedGraphStorage();
            processLevelStorage.connect();
            if (processLevelStorage.graphCache == null) {
                throw new Exception("Fail to create cache");
            }
            isInited = true;
            System.err.println("Process-level cache-based redis graph storage established!");
            cacheStatsReportThread = new Thread(new CacheStatsReportRunnable(processLevelStorage.graphCache, 2));
            cacheStatsReportThread.setDaemon(true);
            cacheStatsReportThread.setName("Cache Stats Reporter");
            cacheStatsReportThread.start();
        }
    }

    public static CacheBasedDistributedGraphStorage getProcessLevelStorage() throws Exception {
        if (isInited) {
            return processLevelStorage;
        } else {
            initProcessLevelCache();
        }
        return processLevelStorage;
    }


    public CacheBasedDistributedGraphStorage(String propertyFile) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(propertyFile));
        cacheCapacityInGB = Double.parseDouble(properties.getProperty("cache.capacity.gb","1"));
    }

    public CacheBasedDistributedGraphStorage() throws IOException {
        Properties properties = ProcessLevelConfReader.getPasaConf();
        cacheCapacityInGB = Double.parseDouble(properties.getProperty("cache.capacity.gb", "1"));
    }

    /**
     * Query the adjacency set of a vertex
     *
     * @param vid
     * @return
     */
    @Override
    final public int[] get(int vid) throws Exception {
        if (graphCache == null) {
            throw new Exception("null cache!");
        }
        return graphCache.get(vid);
    }

    @Override
    final public Map<Integer, int[]> get(int[] vids) {
        return graphCache.getAll(IntArrayList.wrap(vids));
    }

    /**
     * Set the adjacency set of a vertex
     *
     * @param vid
     * @param adj
     */
    @Override
    final public void put(int vid, int[] adj) throws Exception {
        distributedGraphStorage.put(vid, adj);
    }

    @Override
    final public void put(Int2ObjectMap<int[]> vid2Adj) throws Exception {
        distributedGraphStorage.put(vid2Adj);
    }

    @Override
    public void close() {
        graphCache.invalidateAll();
        graphCache.cleanUp();
    }

    @Override
    public void connect() throws Exception {
        distributedGraphStorage =
                new HBaseGraphStorage();
        distributedGraphStorage.connect();
        graphCache = Caffeine.newBuilder()
                .recordStats()
                .maximumWeight((long)(cacheCapacityInGB * 1024 * 1024 * 1024))
                .weigher(new ArrayLengthWeigher())
                .build(new DBCacheLoader(distributedGraphStorage));
        /*triangleCache = Caffeine.newBuilder()
                .maximumWeight((long)(cacheCapacityInGB * 1024 * 1024 * 1024))
                .weigher(new ArrayLengthWeigher())
                .build();*/
    }

    @Override
    public void clearDB() {
        graphCache.invalidateAll();
        graphCache.cleanUp();
        distributedGraphStorage.clearDB();
    }

}

class ArrayLengthWeigher implements Weigher<Object, int[]> {

private final int spaceConsumedByUtilClass = 32 + 40;
private final int vidSize = 16;
private final int objectHeadSize = 16;

    @Override
    final public int weigh(@Nonnull Object vid, @Nonnull int[] adj) {
        return (int)(adj.length * 4  + objectHeadSize + vidSize
                + spaceConsumedByUtilClass);
    }

    public static void main(String args[]) {
        System.out.println(RamUsageEstimator.sizeOf(new ArrayLengthWeigher()));
        System.out.println(RamUsageEstimator.sizeOf(new int[2000]));
    }
}

class DBCacheLoader implements CacheLoader<Integer, int[]> {

    private AbstractSimpleGraphStorage graphStorage;

    public DBCacheLoader(AbstractSimpleGraphStorage storage) throws Exception {
        this.graphStorage = storage;
    }

    @Override
    final public int[] load(@Nonnull Integer vid) throws Exception {
        return graphStorage.get(vid);
    }

    @Nonnull
    @Override
    final public Map<Integer, int[]> loadAll(@Nonnull Iterable<? extends Integer> keys) throws Exception {
        IntArrayList vids = new IntArrayList();
        Iterator<? extends Integer> iter = keys.iterator();
        while (iter.hasNext()) {
            vids.add(iter.next().intValue());
        }
        return graphStorage.get(vids.toIntArray());
    }
}

class CacheStatsReportRunnable implements Runnable {

    private LoadingCache<Integer, int[]> cacheReference = null;
    private long sleepTimeInSecond = 2;

    public CacheStatsReportRunnable(LoadingCache<Integer, int[]> cacheReference) {
        this.cacheReference = cacheReference;
    }

    public CacheStatsReportRunnable(LoadingCache<Integer, int[]> cacheReference, long sleepTimeInSecond) {
        this.cacheReference = cacheReference;
        this.sleepTimeInSecond = sleepTimeInSecond;
    }

    private void writeStatsToFile() {
        try {
            PrintWriter writer = new PrintWriter(new File("/tmp/cache.stats.wzk"));
            writer.println(cacheReference.stats().toString());
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        while (true) {
            //System.err.println(new Date().toString() + "\n" + cacheReference.stats());
            writeStatsToFile();
            try {
                Thread.sleep(sleepTimeInSecond * 1000, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
