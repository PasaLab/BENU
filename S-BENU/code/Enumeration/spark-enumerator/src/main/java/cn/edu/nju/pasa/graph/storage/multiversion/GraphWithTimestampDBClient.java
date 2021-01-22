package cn.edu.nju.pasa.graph.storage.multiversion;

import cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.CommonFunctions;
import cn.edu.nju.pasa.graph.conf.ProcessLevelConfReader;
import com.carrotsearch.sizeof.RamUsageEstimator;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Create by huweiwei on 04/28/2019
 */
public class GraphWithTimestampDBClient {

    private HBaseGraphMultiVersionStorage distributedGraphStorage = null;
    public Cache<Integer, int[]> graphCache = null;
    private double cacheCapacityInGB = 1;
    private boolean enableCache = true;

    private static volatile GraphWithTimestampDBClient processLevelStorage = null;
    private static volatile boolean isInited = false;
    private static boolean processLevelCacheCreateError = false;
    private static Thread cacheStatsReportThread = null;

    public AtomicLong queryCount = new AtomicLong();
    public AtomicLong missCount = new AtomicLong();
    public AtomicLong loadingTime = new AtomicLong();

    private static synchronized void initProcessLevelCache() throws Exception {
        if (!isInited) {
            processLevelStorage = new GraphWithTimestampDBClient();
            processLevelStorage.connect();
            if (processLevelStorage.enableCache && processLevelStorage.graphCache == null) {
                throw new Exception("Fail to create cache");
            }
            isInited = true;
            System.err.println("Process-level cache-based graph storage established!");
            if (processLevelStorage.enableCache) {
                cacheStatsReportThread = new Thread(new CacheStatsReportRunnable(processLevelStorage.queryCount,
                        processLevelStorage.missCount, processLevelStorage.loadingTime, 2));
                cacheStatsReportThread.setDaemon(true);
                cacheStatsReportThread.setName("Cache Stats Reporter");
                cacheStatsReportThread.start();
            }
        }
    }

    public static GraphWithTimestampDBClient getProcessLevelStorage() throws Exception {
        if (isInited) {
            return processLevelStorage;
        } else {
            initProcessLevelCache();
        }
        return processLevelStorage;
    }

    public GraphWithTimestampDBClient(String propertyFile) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(propertyFile));
        cacheCapacityInGB = Double.parseDouble(properties.getProperty("cache.capacity.gb","1"));
        enableCache = properties.getProperty("cache.enable", "true").equalsIgnoreCase("true");
    }

    public GraphWithTimestampDBClient() throws IOException {
        Properties properties = ProcessLevelConfReader.getPasaConf();
        cacheCapacityInGB = Double.parseDouble(properties.getProperty("cache.capacity.gb", "1"));
        enableCache = properties.getProperty("cache.enable", "true").equalsIgnoreCase("true");
    }

    public void close() {
        graphCache.invalidateAll();
        graphCache.cleanUp();
        distributedGraphStorage.close();
    }

    public void connect() throws Exception {
        distributedGraphStorage =
                new HBaseGraphMultiVersionStorage();
        distributedGraphStorage.connect();
        if (enableCache) {
            graphCache = Caffeine.newBuilder()
                    .maximumWeight((long) (cacheCapacityInGB * 1024 * 1024 * 1024))
                    .weigher(new ArrayLengthWeigher())
                    .recordStats()
                    .build();
        }
    }

    public void clearDB() {
        if (graphCache != null) {
            graphCache.invalidateAll();
            graphCache.cleanUp();
        }
        distributedGraphStorage.clearDB();
    }

    public void clearCache() {
        if (graphCache == null) return;
        graphCache.invalidateAll();
        graphCache.cleanUp();
    }

    /**
     * Get the corresponding adj of vid according to AdjType and T
     * @param vid
     * @param type
     * @param T current timestamp
     * @param isInsert if delta edge is inserted, isInsert = true, else isInsert = false;
     * @return
     * @throws Exception
     */
    public int[] get(int vid, AdjType type, int T, boolean isInsert) throws Exception {
        if (type == AdjType.OLD_FORWARD) {
            return getOldForwardAdj(vid, T, isInsert);
        } else if (type == AdjType.OLD_REVERSE) {
            return getOldReverseAdj(vid, T, isInsert);
        } else if (type == AdjType.NEW_FORWARD) {
            return getNewForwardAdj(vid, T, isInsert);
        } else if (type == AdjType.NEW_REVERSE) {
            return getNewReverseAdj(vid, T, isInsert);
        } else {
            throw new Exception("unrecognized adj type!");
        }

    }

    public int[] get(int vid, AdjType type, AdjType direction, boolean isInsert, int T) throws Exception {
        if(isInsert) {
            if(type == AdjType.EITHER) {
                // get either is get new
                if(direction == AdjType.FORWARD) {
                    return getNewForwardAdj(vid, T, isInsert);
                } else {
                    return getNewReverseAdj(vid, T, isInsert);
                }
            } else if(type == AdjType.UNALTERED){
                // get unaltered is get old
                if(direction == AdjType.FORWARD) {
                    return getOldForwardAdj(vid, T, isInsert);
                } else {
                    return getOldReverseAdj(vid, T, isInsert);
                }
            } else {
                throw new Exception("unrecognized adj type!");
            }
        } else {
            if(type == AdjType.EITHER) {
                // get either is get old
                if(direction == AdjType.FORWARD) {
                    return getOldForwardAdj(vid, T, isInsert);
                } else {
                    return getOldReverseAdj(vid, T, isInsert);
                }
            } else if(type == AdjType.UNALTERED){
                // get unaltered is get new
                if(direction == AdjType.FORWARD) {
                    return getNewForwardAdj(vid, T, isInsert);
                } else {
                    return getNewReverseAdj(vid, T, isInsert);
                }
            } else {
                throw new Exception("unrecognized adj type!");
            }
        }
    }

    public void put(int vid, int[] oldForwardAdj, int[] oldReverseAdj, int[] deltaForwardAdj, int[] deltaReverseAdj, int T) throws Exception {
        // if T = 0 , put initial graph into DB, if T > 0, put update graph into DB and cache
        
        // put into DB
        distributedGraphStorage.put(vid, oldForwardAdj, oldReverseAdj, deltaForwardAdj, deltaReverseAdj);
        /*
        if(T > 0) {
            // put into cache
            int[] combinedAdj = generateCombinedAdjInCache(vid, oldForwardAdj, oldReverseAdj, deltaForwardAdj, deltaReverseAdj, T);
            graphCache.put(vid, combinedAdj);
        }
         */
    }

    public Int2ObjectMap<int[][]> get(int[] vids) throws Exception {
        // get from distributed graph storage
        return distributedGraphStorage.get(vids);
    }

    public void put(Int2ObjectMap<int[][]> vid2Adjs, int T) throws Exception {
        distributedGraphStorage.put(vid2Adjs);
        /*
        if(T > 0) { // if T = 0 , put initial graph into DB, if T > 0, put update graph into DB and cache
            for (int vid : vid2Adjs.keySet()) {
                int[][] adjs = vid2Adjs.get(vid);
                int[] combinedAdj = generateCombinedAdjInCache(vid, adjs[0], adjs[1], adjs[2], adjs[3], T);
                graphCache.put(vid, combinedAdj);
            }
        }
         */
    }

    private int[] getOldForwardAdj(int vid, int T, boolean isInsert) throws Exception{
        queryCount.addAndGet(1L);
        // we need the adj with timestamp T-1
        int[] cacheAdj = null;
        if (graphCache != null) cacheAdj = graphCache.getIfPresent(vid);
        if (cacheAdj != null) {
            int t = cacheAdj[0];
            if (t == T - 1 && !isInsert) {
                // return newForwardAdj in cacheAdj
                return setAllVerticesSignBitTo0(cacheAdj, cacheAdj[2], cacheAdj[3]);
            } else if (t == T) {
                // return oldForwardAdj in cacheAdj
                if(isInsert) {
                    // if the delta edge is inserted edge, filter vertices in oldForwardAdj whose sign bit is 1
                    return filterVerticesWithSignBit1(cacheAdj, 4, cacheAdj[1]);
                } else {
                    return setAllVerticesSignBitTo0(cacheAdj, 4, cacheAdj[1]);
                }
            }
        }
        missCount.addAndGet(1L);
        long t0 = System.nanoTime();
        // get newest adj from distributed graph storage
        int[][] adjs = distributedGraphStorage.get(vid);
        int[] combinedAdj = generateCombinedAdjInCache(vid, adjs[0], adjs[1], adjs[2], adjs[3], T);
        if (graphCache != null)
            graphCache.put(vid, combinedAdj);
        long t1 = System.nanoTime();
        loadingTime.addAndGet(t1 - t0);

        // return oldForwardAdj in combinedAdj
        if(isInsert) {
            // if the delta edge is inserted edge, filter vertices in oldForwardAdj whose sign bit is 1
            return filterVerticesWithSignBit1(combinedAdj, 4, combinedAdj[1]);
        } else return adjs[0];
    }

    private int[] getOldReverseAdj(int vid, int T, boolean isInsert) throws Exception{
        queryCount.addAndGet(1L);
        // we need the adj with timestamp T-1
        int[] cacheAdj = null;
        if (graphCache != null) cacheAdj = graphCache.getIfPresent(vid);
        if (cacheAdj != null) {
            int t = cacheAdj[0];
            if (t == T - 1 && !isInsert) {
                // return newReverseAdj in cacheAdj
                return setAllVerticesSignBitTo0(cacheAdj, cacheAdj[3], cacheAdj.length);
            } else if (t == T) {
                // return oldReverseAdj in cacheAdj
                if(isInsert) {
                    return filterVerticesWithSignBit1(cacheAdj, cacheAdj[1], cacheAdj[2]);
                } else {
                    return setAllVerticesSignBitTo0(cacheAdj, cacheAdj[1], cacheAdj[2]);
                }
            }
        }
        missCount.addAndGet(1L);
        long t0 = System.nanoTime();
        // get newest adj from distributed graph storage
        int[][] adjs = distributedGraphStorage.get(vid);
        int[] combinedAdj = generateCombinedAdjInCache(vid, adjs[0], adjs[1], adjs[2], adjs[3], T);
        if (graphCache != null)
            graphCache.put(vid, combinedAdj);
        long t1 = System.nanoTime();
        loadingTime.addAndGet(t1 - t0);
        if(isInsert) return filterVerticesWithSignBit1(combinedAdj, combinedAdj[1], combinedAdj[2]);
        else return adjs[1];
    }

    private int[] getNewForwardAdj(int vid, int T, boolean isInsert) throws Exception{
        queryCount.addAndGet(1L);
        // we need the adj with timestamp T
        int[] cacheAdj = null;
        if (graphCache != null) cacheAdj = graphCache.getIfPresent(vid);
        if (cacheAdj != null) {
            int t = cacheAdj[0];
            if (t == T) {
                // return newForwardAdj in cacheAdj
                if(isInsert) {
                    return setAllVerticesSignBitTo0(cacheAdj, cacheAdj[2], cacheAdj[3]);
                } else {
                    // if the delta edge is deleted edge, filter vertices in newForwardAdj whose sign bit is 1
                    return filterVerticesWithSignBit1(cacheAdj, cacheAdj[2], cacheAdj[3]);
                }
            }
        }
        missCount.addAndGet(1L);
        long t0 = System.nanoTime();
        // get newest adj from distributed graph storage
        int[][] adjs = distributedGraphStorage.get(vid);
        int[] combinedAdj = generateCombinedAdjInCache(vid, adjs[0], adjs[1], adjs[2], adjs[3], T);
        if (graphCache != null)
            graphCache.put(vid, combinedAdj);
        long t1 = System.nanoTime();
        loadingTime.addAndGet(t1 - t0);
        if(isInsert) return setAllVerticesSignBitTo0(combinedAdj, combinedAdj[2], combinedAdj[3]);
        else return filterVerticesWithSignBit1(combinedAdj, combinedAdj[2], combinedAdj[3]);
    }

    private int[] getNewReverseAdj(int vid, int T, boolean isInsert) throws Exception{
        queryCount.addAndGet(1L);
        // we need the adj with timestamp T
        int[] cacheAdj = null;
        if (graphCache != null) cacheAdj = graphCache.getIfPresent(vid);
        if (cacheAdj != null) {
            int t = cacheAdj[0];
            if (t == T) {
                // return newReverseAdj in cacheAdj
                if(isInsert) {
                    return setAllVerticesSignBitTo0(cacheAdj, cacheAdj[3], cacheAdj.length);
                } else {
                    // if the delta edge is deleted edge, filter vertices in newReverseAdj whose sign bit is 1
                    return filterVerticesWithSignBit1(cacheAdj, cacheAdj[3], cacheAdj.length);
                }
            }
        }
        missCount.addAndGet(1L);
        long t0 = System.nanoTime();
        // get newest adj from distributed graph storage
        int[][] adjs = distributedGraphStorage.get(vid);
        int[] combinedAdj = generateCombinedAdjInCache(vid, adjs[0], adjs[1], adjs[2], adjs[3], T);
        if (graphCache != null)
            graphCache.put(vid, combinedAdj);
        long t1 = System.nanoTime();
        loadingTime.addAndGet(t1 - t0);
        if(isInsert) return setAllVerticesSignBitTo0(combinedAdj, combinedAdj[3], combinedAdj.length);
        else return filterVerticesWithSignBit1(combinedAdj, combinedAdj[3], combinedAdj.length);
    }

    public int[] generateCombinedAdjInCache(int vid, int[] oldForwardAdj, int[] oldReverseAdj,
                                            int[] deltaForwardAdj, int[] deltaReverseAdj, int T) throws Exception{
        int[][] oldAndNewForwardAdjWithFlag = CommonFunctions.generateOldAndNewSortedArrays(oldForwardAdj, deltaForwardAdj);
        int[] oldForwardAdjWithFlag = oldAndNewForwardAdjWithFlag[0];
        int[] newForwardAdjWithFlag = oldAndNewForwardAdjWithFlag[1];

        int[][] oldAndNewReverseAdjWithFlag = CommonFunctions.generateOldAndNewSortedArrays(oldReverseAdj, deltaReverseAdj);
        int[] oldReverseAdjWithFlag = oldAndNewReverseAdjWithFlag[0];
        int[] newReverseAdjWithFlag = oldAndNewReverseAdjWithFlag[1];

        int[] combinedAdj = new int[4 + oldForwardAdjWithFlag.length + oldReverseAdjWithFlag.length +
                newForwardAdjWithFlag.length + newReverseAdjWithFlag.length];
        combinedAdj[0] = T;
        combinedAdj[1] = 4 + oldForwardAdjWithFlag.length;
        combinedAdj[2] = 4 + oldForwardAdjWithFlag.length + oldReverseAdjWithFlag.length;
        combinedAdj[3] = 4 + oldForwardAdjWithFlag.length + oldReverseAdjWithFlag.length + newForwardAdjWithFlag.length;

        System.arraycopy(oldForwardAdjWithFlag, 0, combinedAdj, 4, oldForwardAdjWithFlag.length);
        System.arraycopy(oldReverseAdjWithFlag, 0, combinedAdj, combinedAdj[1], oldReverseAdjWithFlag.length);
        System.arraycopy(newForwardAdjWithFlag, 0, combinedAdj, combinedAdj[2], newForwardAdjWithFlag.length);
        System.arraycopy(newReverseAdjWithFlag, 0, combinedAdj, combinedAdj[3], newReverseAdjWithFlag.length);
        return combinedAdj;
    }

    private int[] setAllVerticesSignBitTo0(int[] combinedAdj, int start, int end) {
        int[] result = new int[end - start];
        int j = 0;
        for(int i = start; i < end; i++) {
            result[j++] = CommonFunctions.setSignBitTo0(combinedAdj[i]);
        }
        return result;
    }

    private int[] filterVerticesWithSignBit1(int[] combinedAdj, int start, int end) {
        IntArrayList result = new IntArrayList();
        for(int i = start; i < end; i++) {
            if(CommonFunctions.isSignBit1(combinedAdj[i])) continue;
            result.add(combinedAdj[i]);
        }
        return result.toIntArray();
    }
}

class ArrayLengthWeigher implements Weigher<Object, int[]> {

    private final int spaceConsumedByUtilClass = 32 + 40;
    private final int vidSize = 16;
    private final int objectHeadSize = 16;

    @Override
    final public int weigh(@Nonnull Object vid, @Nonnull int[] adj) {
        return (int) (adj.length * 4 + objectHeadSize + vidSize
                + spaceConsumedByUtilClass);
    }

    public static void main(String args[]) {
        System.out.println(RamUsageEstimator.sizeOf(new ArrayLengthWeigher()));
        System.out.println(RamUsageEstimator.sizeOf(new int[2000]));
    }
}

class CacheStatsReportRunnable implements Runnable {

    private AtomicLong queryCount, missCount, loadingTime;
    private long sleepTimeInSecond = 1;


    public CacheStatsReportRunnable(AtomicLong queryCount, AtomicLong missCount, AtomicLong loadingTime) {
        this(queryCount, missCount, loadingTime,2);
    }

    public CacheStatsReportRunnable(AtomicLong queryCount, AtomicLong missCount, AtomicLong loadingTime, long sleepTimeInSecond) {
        this.queryCount = queryCount;
        this.missCount = missCount;
        this.loadingTime = loadingTime;
        this.sleepTimeInSecond = sleepTimeInSecond;
    }

    private void writeStatsToFile() {
        try {
            PrintWriter writer = new PrintWriter(new File("/tmp/dynamic.cache.stats.wzk"));
            writer.println(this.queryCount.longValue()
                    + "," + (this.queryCount.longValue() - this.missCount.longValue())
                    + "," + this.missCount.longValue() + "," + this.loadingTime.longValue());
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

