package cn.edu.nju.pasa.graph.analysis.subgraph;

import cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.*;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.analysis.subgraph.util.ClusterStatusServer;
import cn.edu.nju.pasa.graph.analysis.subgraph.util.CommandLineUtils;
import cn.edu.nju.pasa.graph.storage.multiversion.GraphWithTimestampDBClient;
import cn.edu.nju.pasa.graph.util.HDFSUtil;
import cn.edu.nju.pasa.graph.util.HostInfoUtil;
import cn.edu.nju.pasa.graph.util.ProcessLevelThreadPool;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class DynamicSubgraphEnumerationGeneric2 {

    private static Partitioner globalPartitioner;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("Dynamic Subgraph Enumeration - Generic")
                .set("spark.ui.showConsoleProgress", "false");
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        JavaSparkContext javasc = new JavaSparkContext(conf);
        int numExecutors = javasc.getConf().getInt("spark.executor.instances", 16);
        int numCoresPerExecutor = javasc.getConf().getInt("spark.executor.cores", 24);


        Properties jobProp = CommandLineUtils.parseArgs(args);
        jobProp.setProperty(MyConf.NUM_EXECUTORS, Integer.toString(numExecutors));
        String initialForwardGraphPath = jobProp.getProperty(MyConf.INITIAL_FORWARD_GRAPH_PATH);
        String initialReverseGraphPath = jobProp.getProperty(MyConf.INITIAL_REVERSE_GRAPH_PATH);
        String updateEdgesPath = jobProp.getProperty(MyConf.UPDATE_EDGES_PATH);
        int updateEdgesBatch = Integer.parseInt(jobProp.getProperty(MyConf.UPDATE_EDGES_BATCH));
        int parallelism = Integer.parseInt(jobProp.getProperty(MyConf.PARTITION_NUM));
        int batchSize = Integer.parseInt(jobProp.getProperty(MyConf.BLOCKING_QUEUE_SIZE));
        boolean enableBalance = Boolean.parseBoolean(jobProp.getProperty(MyConf.ENABLE_LOAD_BALANCE, "false"));
        boolean isAdaptiveThreshold = Boolean.parseBoolean(jobProp.getProperty(MyConf.ENABLE_ADAPTIVE_BALANCE_THRESHOLD, "false"));

        globalPartitioner = new HashPartitioner(parallelism);

        GraphWithTimestampDBClient graphStorage = GraphWithTimestampDBClient.getProcessLevelStorage();

        System.err.println("Clear the database...");
        try {
            graphStorage.clearDB();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(101);
        }

        long start = System.nanoTime();
        System.err.println("Store the initial data graph...");
        int logicalTimestamp = 0;

        // adj file: vid adj1 adj2 ...
        JavaPairRDD<Integer, int[]> initForwardAdjRDD = readAdjFile(javasc, initialForwardGraphPath, parallelism);
        JavaPairRDD<Integer, int[]> initReverseAdjRDD = readAdjFile(javasc, initialReverseGraphPath, parallelism);

        JavaPairRDD<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> initAdjsRDD =
                initForwardAdjRDD.fullOuterJoin(initReverseAdjRDD);
        LongAccumulator numEdgeAccu = javasc.sc().longAccumulator("number of edge");
        LongAccumulator numVertexAccu = javasc.sc().longAccumulator("number of vertex");
        writeInitGraphToDB(initAdjsRDD, batchSize, numVertexAccu, numEdgeAccu);

        long t0 = System.nanoTime();
        System.err.println("Initial storage elapsed time (s): " + (t0 - start)/1e9);

        double initialStorageTime = (t0 - start)/1e9;
        double updateStorageTime = 0;
        double enumerationTime = 0;
        double mergeUpdateTime = 0;

        System.err.println("Load execution plans...");
        //String patternGraphName = jobProp.getProperty(MyConf.PATTERN_GRAPH_NAME);
        //int patternEdgeNum = Integer.parseInt(jobProp.getProperty(MyConf.PATTERN_EDGE_NUM));
        String incrementalExecPlansPath = jobProp.getProperty(MyConf.INCREMENTAL_EXEC_PLANS_PATH);
        File file = new File(incrementalExecPlansPath);
        File[] fs = file.listFiles();
        Arrays.sort(fs, (a, b) -> a.getName().compareTo(b.getName()));
        IncrementalExecutionPlan executionPlans[] = new IncrementalExecutionPlan[fs.length];
        for(int i = 0; i < fs.length; i++) {
            File f = fs[i];
            System.err.println("execPlan: " + f.getPath());
            String[] execPlan = loadExecutionPlan(f);
            IncrementalExecutionPlan iPlan = new IncrementalExecutionPlan(execPlan);
            executionPlans[i] = iPlan;
        }

        Broadcast<IncrementalExecutionPlan[]> broadcastPlans = javasc.broadcast(executionPlans);
        // Start the cluster status server
        /*
        ClusterStatusServer clusterStatusServer = new ClusterStatusServer(
                numExecutors * numCoresPerExecutor,
                () -> {
                    JavaSparkStatusTracker statusTracker = javasc.statusTracker();
                    if (statusTracker.getActiveStageIds().length == 0) return 0;
                    int activeStageID = statusTracker.getActiveStageIds()[0];
                    SparkStageInfo stageInfo = statusTracker.getStageInfo(activeStageID);
                    //System.out.println("Active stage:" + activeStageID + ", #active task:" + stageInfo.numActiveTasks());
                    return stageInfo.numActiveTasks();
                });
        Server grpcServer = ServerBuilder.forPort(8383).addService(clusterStatusServer).build();
        grpcServer.start();
        jobProp.setProperty(MyConf.DRIVER_HOSTNAME, HostInfoUtil.getHostName());
        System.out.println("Cluster status server starts...");
         */


        Long totalDeltaEmbeddingNum = 0L;

        Path updatesFilePath = new Path(updateEdgesPath);
        try {
            LongAccumulator numDeltaEdgeAccu = javasc.sc().longAccumulator("number of delta edge");
            LongAccumulator numDeltaVertexAccu = javasc.sc().longAccumulator("number of delta vertex");
            for (Path filePath : HDFSUtil.listFiles(updatesFilePath.toString())) {
                if (filePath.getName().startsWith("_")) continue; // skip the files starting with _
                logicalTimestamp++;
                if(logicalTimestamp > updateEdgesBatch) break;
                jobProp.setProperty(MyConf.TIMESTAMP, String.valueOf(logicalTimestamp));
                t0 = System.nanoTime();

                System.err.println("---------------- Timestamp " + logicalTimestamp + "----------------");
                System.err.println("update edges path: " + updateEdgesPath + "/" + filePath.getName());

                System.err.println("Store the update edges...");
                // update edge file : src dst 1/-1
                JavaPairRDD<Integer, Integer> deltaForwardEdgeRDD = readEdgeFileToForwardEdges(javasc,
                        updateEdgesPath + "/" + filePath.getName(), parallelism,
                        numEdgeAccu);
                JavaPairRDD<Integer, int[]> deltaForwardAdjRDD = edgesToAdjs(deltaForwardEdgeRDD);
                deltaForwardAdjRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
                // edge file: src dst

                JavaPairRDD<Integer, Integer> deltaReverseEdgeRDD = readEdgeFileToReverseEdges(javasc,
                        updateEdgesPath + "/" + filePath.getName(), parallelism);
                JavaPairRDD<Integer, int[]> deltaReverseAdjRDD = edgesToAdjs(deltaReverseEdgeRDD);

                JavaPairRDD<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> deltaAdjsRDD = deltaForwardAdjRDD
                        .fullOuterJoin(deltaReverseAdjRDD);
                deltaAdjsRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
                numDeltaVertexAccu.reset();
                numDeltaEdgeAccu.reset();
                writeDeltaEdgesToDB(deltaAdjsRDD, batchSize, logicalTimestamp, numDeltaVertexAccu, numDeltaEdgeAccu);

                long t1 = System.nanoTime();
                //System.err.println("Store the update edges time (s): " + (t1 - t0)/1e9);
                updateStorageTime = updateStorageTime + (t1 - t0)/1e9;

                System.err.println("Enumerate...");

                if (enableBalance && isAdaptiveThreshold) {
                    setBalanceThreshold(jobProp, executionPlans[0].patternVertexNum, numVertexAccu.value(),
                            numEdgeAccu.value(), numDeltaVertexAccu.value(), numDeltaEdgeAccu.value());
                }

                totalDeltaEmbeddingNum += enumerate(javasc, deltaForwardAdjRDD, jobProp, broadcastPlans);

                long t2 = System.nanoTime();
                //System.err.println("Enumerate time (s): " + (t2 - t1)/1e9);
                enumerationTime = enumerationTime + (t2 - t1)/1e9;

                System.err.println("Merge delta adjs...");
                JavaRDD<Integer> deltaVidsRDD = deltaAdjsRDD.keys();
                mergeUpdates(deltaVidsRDD, batchSize);
                deltaForwardAdjRDD.unpersist();
                deltaAdjsRDD.unpersist();

                long t3 = System.nanoTime();
                //System.err.println("Merge delta adjs time (s): " + (t3 - t2)/1e9);
                mergeUpdateTime = mergeUpdateTime + (t3 - t2)/1e9;

                double time = (t3 - t0) / 1e9;
                String statLine = String.format("Timestamp %d consumes %.2f seconds to process.", logicalTimestamp, time);
                System.err.println(statLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(102);
        }

        long end = System.nanoTime();

        /*
        System.err.println("-----------------------------------------------");
        for(int i = 0; i < intermidiateResultsAccus.length; i++) {
            System.err.println("task" + (i / 5) + ".f" + (i % 5) + ": " + intermidiateResultsAccus[i].value());
        }

         */

        System.err.println("-----------------------------------------------");
        System.err.println("Initial storage elapsed time (s): " + initialStorageTime);
        System.err.println("Store the update edges time (s): " + updateStorageTime);
        System.err.println("Enumerate time (s): " + enumerationTime);
        System.err.println("Merge delta adjs time (s): " + mergeUpdateTime);
        System.err.println("Total elapsed time (s): " + (end - start)/1e9);
        System.err.println("Total delta embedding num: " + totalDeltaEmbeddingNum);

        javasc.stop();
        /*
        clusterStatusServer.shutdownNow();
        clusterStatusServer.awaitTermination();
         */
        System.exit(0);
    }

    public static void setBalanceThreshold(Properties jobProp, int numPatternVertex, long numVertex, long numEdge,
                                           long numDeltaVertex, long numDeltaEdge) {
        double avg_delta_deg = Math.ceil((double)numDeltaEdge / numDeltaVertex);
        double avg_deg = Math.ceil((double)numEdge / numVertex);
        int balanceThreshold1[] = new int[numPatternVertex - 2];
        int balanceThreshold2[] = new int[numPatternVertex - 2];
        balanceThreshold1[0] = (int)(10 * avg_delta_deg);
        balanceThreshold2[0] = (int)(2 * avg_delta_deg);
        if (numPatternVertex >= 4) {
            balanceThreshold1[1] = (int)(10 * avg_deg);
            balanceThreshold2[1] = (int)(2 * avg_deg);
            for (int i = 2; i < numPatternVertex - 2; i++) {
                balanceThreshold1[i] = balanceThreshold1[i-1] * 10;
                balanceThreshold2[i] = balanceThreshold2[i-1] * 10;
            }
        }
        String balanceThresholdConf1 = generateCommaSeparatedString(balanceThreshold1);
        String balanceThresholdConf2 = generateCommaSeparatedString(balanceThreshold2);
        jobProp.setProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTERNODE, balanceThresholdConf1);
        jobProp.setProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTRANODE, balanceThresholdConf2);
        System.out.println("Set inter-node balance threshold:" + balanceThresholdConf1);
        System.out.println("Set intra-node balance threshold:" + balanceThresholdConf2);
    }

    public static JavaPairRDD<Integer, int[]> readAdjFile(JavaSparkContext javasc, String graphFilePath, int parallelism) {
        JavaRDD<String> adjFile = javasc.textFile(graphFilePath, parallelism);
        JavaPairRDD<Integer, int[]> adjs = adjFile.mapToPair(line ->  {
            String[] fields = line.split(" ");
            int vid = Integer.parseInt(fields[0]);
            int[] adj = new int[fields.length - 1];
            for (int i = 1; i < fields.length; i++) {
                adj[i-1] = Integer.parseInt(fields[i]);
            }
            Arrays.sort(adj);
            return new Tuple2<>(vid, adj);
        });
        return adjs;
    }

    public static JavaPairRDD<Integer, Integer> readEdgeFile(JavaSparkContext javasc, String graphFilePath, int parallelism) {
        JavaRDD<String> edgeFile = javasc.textFile(graphFilePath, parallelism);
        JavaPairRDD<Integer, Integer> edges = edgeFile.mapToPair(line ->  {
            String[] fields = line.split(" ");
            int src = Integer.parseInt(fields[0]);
            int dst = Integer.parseInt(fields[1]);
            if(fields.length > 2) { // if fileds.length <= 2, insert the edge(src,dst) by default
                if(Integer.parseInt(fields[2]) == -1) { // delete the edge(src,dst)
                    dst = CommonFunctions.setSignBitTo1(dst); // set the sign bit to 1 to represent deleted edge
                }
            }
            return new Tuple2<>(src, dst);
        });
        return edges;
    }

    public static JavaPairRDD<Integer, Integer> readEdgeFileToForwardEdges(JavaSparkContext javasc, String graphFilePath,
                                                                           int parallelism, LongAccumulator numEdgeAccu) {
        JavaRDD<String> edgeFile = javasc.textFile(graphFilePath, parallelism);
        JavaPairRDD<Integer, Integer> edges = edgeFile.mapToPair(line ->  {
            String[] fields = line.split(" ");
            int src = Integer.parseInt(fields[0]);
            int dst = Integer.parseInt(fields[1]);
            int w = Integer.parseInt(fields[2]);
            if (w == 1) {
                numEdgeAccu.add(1L);
            } else {
                numEdgeAccu.add(-1L);
            }
            if (w == -1) { // delete the edge(src,dst)
                dst = CommonFunctions.setSignBitTo1(dst); // set the sign bit to 1 to represent deleted edge
            }
            return new Tuple2<>(src, dst);
        });
        return edges;
    }

    public static JavaPairRDD<Integer, Integer> readEdgeFileToReverseEdges(JavaSparkContext javasc, String graphFilePath, int parallelism) {
        JavaRDD<String> edgeFile = javasc.textFile(graphFilePath, parallelism);
        JavaPairRDD<Integer, Integer> edges = edgeFile.mapToPair(line ->  {
            String[] fields = line.split(" ");
            int src = Integer.parseInt(fields[0]);
            int dst = Integer.parseInt(fields[1]);
            int w = Integer.parseInt(fields[2]);
            if (w == -1) { // delete the edge(src,dst)
                src = CommonFunctions.setSignBitTo1(src);  // set the sign bit to 1 to represent deleted edge
            }
            return new Tuple2<>(dst, src);
        });
        return edges;
    }

    public static JavaPairRDD<Integer, int[]> edgesToAdjs(JavaPairRDD<Integer, Integer> edgesRDD){
        JavaPairRDD<Integer, int[]> adjs = edgesRDD.groupByKey(globalPartitioner).mapValues(e -> {
            IntArrayList adj = new IntArrayList();
            for(int v: e) adj.add(v);
            Collections.sort(adj, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return CommonFunctions.setSignBitTo0(o1) - CommonFunctions.setSignBitTo0(o2);
                }
            });
            int[] arr = adj.toIntArray();
            return arr;
        });

        return adjs;
    }

    public static void writeInitGraphToDB(JavaPairRDD<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> initAdjsRDD,
                                          int batchSize, LongAccumulator numVertexAccu, LongAccumulator numEdgeAccu) {
        initAdjsRDD.foreachPartition(partition -> {
            GraphWithTimestampDBClient graphStorage = GraphWithTimestampDBClient.getProcessLevelStorage();
            Int2ObjectMap<int[][]> records = new Int2ObjectOpenHashMap<>();
            while (partition.hasNext()) {
                Tuple2<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> record = partition.next();
                numVertexAccu.add(1L);
                if (record._2()._1().isPresent()) {
                    //  account the number of edges only with one direction
                    numEdgeAccu.add(record._2()._1().get().length);
                }
                int vid = record._1();
                int[][] adjs = new int[4][];

                if(record._2()._1().isPresent()) {
                    adjs[0] = record._2()._1().get(); // old forward adj
                } else adjs[0] = new int[0];

                if(record._2()._2().isPresent()) {
                    adjs[1] = record._2()._2().get(); // old reverse adj
                } else adjs[1] = new int[0];

                adjs[2] = new int[0]; // delta forward adj
                adjs[3] = new int[0]; // delta reverse adj
                records.put(vid, adjs);
                if(records.size() >= batchSize) {
                    try {
                        graphStorage.put(records, 0);
                        records.clear();
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new IOException(e);
                    }
                }
            }
            if (records.size() > 0) {
                try {
                    graphStorage.put(records, 0);
                    records.clear();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException(e);
                }

            }
        });
        System.out.println("Init Graph Statistics: |V|=" + numVertexAccu.value() + ", |E|=" + numEdgeAccu.value()
                + ", avg_deg=" + ((double)numEdgeAccu.value() / numVertexAccu.value()));
    }

    public static void checkAdjValidness(int vid, int oldFAdj[], int oldRAdj[], int deltaFAdj[], int deltaRAdj[]) throws Exception {
        // check forward
        IntSet oldAdjSet = new IntArraySet();
        oldAdjSet.addAll(new IntArrayList(oldFAdj));
        for (int i = 0; i < deltaFAdj.length; i++) {
            int dvid = deltaFAdj[i];
            int actualVid = CommonFunctions.setSignBitTo0(dvid);
            if (CommonFunctions.isSignBit1(dvid)) { // it is a delete edge
                if (!oldAdjSet.contains(actualVid)) {
                    throw new Exception("(vid: " + vid + "): -delta vid " + actualVid
                            + " must appear in old forward adj: " + CommonFunctions.toStr(oldFAdj));
                }
            } else { // it is an inserting edge
                if (oldAdjSet.contains(actualVid)) {
                    throw new Exception("(vid: " + vid + "): +delta vid " + actualVid
                            + " must not appear in old forward adj: " + CommonFunctions.toStr(oldFAdj));
                }
            }
        }
        // check reverse
        oldAdjSet.clear();
        oldAdjSet.addAll(new IntArrayList(oldRAdj));
        for (int i = 0; i < deltaRAdj.length; i++) {
            int dvid = deltaRAdj[i];
            int actualVid = CommonFunctions.setSignBitTo0(dvid);
            if (CommonFunctions.isSignBit1(dvid)) { // it is a delete edge
                if (!oldAdjSet.contains(actualVid)) {
                    throw new Exception("(vid: " + vid + "): delta R adj = " + CommonFunctions.toStr(deltaRAdj)
                            +" -delta vid " + actualVid
                            + " must appear in old reverse adj: " + CommonFunctions.toStr(oldRAdj));
                }
            } else { // it is an inserting edge
                if (oldAdjSet.contains(actualVid)) {
                    throw new Exception("(vid: " + vid + "): +delta vid " + actualVid
                            + " must not appear in old reverse adj: " + CommonFunctions.toStr(oldRAdj));
                }
            }
        }
    }

    public static void writeDeltaEdgesToDB(JavaPairRDD<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> deltaAdjRDD,
                                           int batchSize, int T, LongAccumulator deltaNumVertexAccu,
                                           LongAccumulator deltaNumEdgeAccu) {

        deltaAdjRDD.foreachPartition(partition -> {
            GraphWithTimestampDBClient graphStorage = GraphWithTimestampDBClient.getProcessLevelStorage();
            graphStorage.clearCache();
            Int2ObjectMap<int[][]> deltaAdjs = new Int2ObjectOpenHashMap<>();
            try {
                while (partition.hasNext()) {
                    Tuple2<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> record = partition.next();
                    deltaNumVertexAccu.add(1L);
                    if (record._2()._1.isPresent()) {
                        // only consider one direction when count the number of delta edges
                        deltaNumEdgeAccu.add(record._2()._1.get().length);
                    }
                    int vid = record._1();
                    int[][] adjs = new int[2][];

                    if(record._2()._1().isPresent()) {
                        adjs[0] = record._2()._1().get(); // delta forward adj
                    } else adjs[0] = new int[0];

                    if(record._2()._2().isPresent()) {
                        adjs[1] = record._2()._2().get(); // delta reverse adj
                    } else adjs[1] = new int[0];
                    deltaAdjs.put(vid, adjs);

                    if(deltaAdjs.size() >= batchSize) {
                        int[] vids = deltaAdjs.keySet().toIntArray();
                        Int2ObjectMap<int[][]> newRecords = new Int2ObjectOpenHashMap<>();
                        Int2ObjectMap<int[][]> oldRecords = graphStorage.get(vids);
                        for(int v : vids) {
                            int[][] oldAdj = oldRecords.get(v);
                            int[][] deltaAdj = deltaAdjs.get(v);
                            int[][] newAdjs = new int[4][];
                            newAdjs[0] = oldAdj[0]; // old forward adj
                            newAdjs[1] = oldAdj[1]; // old reverse adj
                            newAdjs[2] = deltaAdj[0]; // delta forward adj
                            newAdjs[3] = deltaAdj[1]; // delta reverse adj
                            checkAdjValidness(v, oldAdj[0], oldAdj[1], deltaAdj[0], deltaAdj[1]);
                            graphStorage.generateCombinedAdjInCache(v, oldAdj[0], oldAdj[1], deltaAdj[0], deltaAdj[1], 0);
                            newRecords.put(v, newAdjs);
                        }
                        graphStorage.put(newRecords, T);
                        deltaAdjs.clear();
                    }
                }

                if(deltaAdjs.size() > 0) {
                    int[] vids = deltaAdjs.keySet().toIntArray();
                    Int2ObjectMap<int[][]> newRecords = new Int2ObjectOpenHashMap<>();
                    Int2ObjectMap<int[][]> oldRecords = graphStorage.get(vids);
                    for(int v : vids) {
                        int[][] oldAdj = oldRecords.get(v);
                        int[][] deltaAdj = deltaAdjs.get(v);
                        int[][] newAdjs = new int[4][];
                        newAdjs[0] = oldAdj[0]; // old forward adj
                        newAdjs[1] = oldAdj[1]; // old reverse adj
                        newAdjs[2] = deltaAdj[0]; // delta forward adj
                        newAdjs[3] = deltaAdj[1]; // delta reverse adj
                        checkAdjValidness(v, oldAdj[0], oldAdj[1], deltaAdj[0], deltaAdj[1]);
                        graphStorage.generateCombinedAdjInCache(v, oldAdj[0], oldAdj[1], deltaAdj[0], deltaAdj[1], 0);
                        newRecords.put(v, newAdjs);
                    }
                    graphStorage.put(newRecords, T);
                    deltaAdjs.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(103);
            }
        });
        System.out.println("Delta Graph Statistics: |V|=" + deltaNumVertexAccu.value()
                + ", |E|=" + deltaNumEdgeAccu.value()
                + ", avg_deg=" + ((double)deltaNumEdgeAccu.value() / deltaNumVertexAccu.value()));
    }

    public static void mergeUpdates(JavaRDD<Integer> deltaVidsRDD, int batchSize) {
        deltaVidsRDD.foreachPartition(partition -> {
            GraphWithTimestampDBClient graphStorage = GraphWithTimestampDBClient.getProcessLevelStorage();
            IntArrayList vids = new IntArrayList();
            try {
                while (partition.hasNext()) {
                    int deltaVid = partition.next();
                    vids.add(deltaVid);

                    if(vids.size() >= batchSize) {
                        Int2ObjectMap<int[][]> oldRecords = graphStorage.get(vids.toIntArray());
                        Int2ObjectMap<int[][]> newRecords = new Int2ObjectOpenHashMap<>();
                        for(int v : vids) {
                            int[][] oldAdj = oldRecords.get(v);
                            int[][] newAdjs = new int[4][];
                            newAdjs[0] = CommonFunctions.mergeOldAndDeltaSortedArrays(oldAdj[0], oldAdj[2]); // merge old and delta forward adj
                            newAdjs[1] = CommonFunctions.mergeOldAndDeltaSortedArrays(oldAdj[1], oldAdj[3]); // merge old and delta reverse adj
                            newAdjs[2] = new int[0];
                            newAdjs[3] = new int[0];
                            newRecords.put(v, newAdjs);
                        }
                        graphStorage.put(newRecords, 0);
                        vids.clear();
                    }
                }

                if(vids.size() > 0) {
                    Int2ObjectMap<int[][]> oldRecords = graphStorage.get(vids.toIntArray());
                    Int2ObjectMap<int[][]> newRecords = new Int2ObjectOpenHashMap<>();
                    for(int v : vids) {
                        int[][] oldAdj = oldRecords.get(v);
                        int[][] newAdjs = new int[4][];
                        newAdjs[0] = CommonFunctions.mergeOldAndDeltaSortedArrays(oldAdj[0], oldAdj[2]); // merge old and delta forward adj
                        newAdjs[1] = CommonFunctions.mergeOldAndDeltaSortedArrays(oldAdj[1], oldAdj[3]); // merge old and delta reverse adj
                        newAdjs[2] = new int[0];
                        newAdjs[3] = new int[0];
                        newRecords.put(v, newAdjs);
                    }
                    graphStorage.put(newRecords, 0);
                    vids.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(104);
            }
        });
    }



    public static Long enumerate(JavaSparkContext javasc,
                                 JavaPairRDD<Integer, int[]> updateVertexToAdjRDD,
                                 Properties jobProp,
                                 Broadcast<IncrementalExecutionPlan[]> broadcastPlans) throws Exception {

        LongAccumulator deltaEmbeddingNumAccu = javasc.sc().longAccumulator("delta.embeddings.num");
        LongAccumulator deltaVertexNumAccu = javasc.sc().longAccumulator("delta.vertex.num");
        LongAccumulator vertexTaskNumAccu = javasc.sc().longAccumulator("vertex.task.num");
        LongAccumulator executionTimeAccu = javasc.sc().longAccumulator("serialExecutionTime(ns)");
        LongAccumulator numINTExecutedAccu = javasc.sc().longAccumulator("number of executed INT instruction");
        LongAccumulator numDBQExecutedAccu = javasc.sc().longAccumulator("number of executed DBQ instruction");
        LongAccumulator numENUExecutedAccu = javasc.sc().longAccumulator("number of executed ENU instruction");
        int partitionNum = Integer.parseInt(jobProp.getProperty(MyConf.PARTITION_NUM));
        int numWorkingThreads = Integer.parseInt(jobProp.getProperty(MyConf.NUM_WORKING_THREADS, "4"));

        JavaRDD<LocalSearchSubTask> tasksRDD = updateVertexToAdjRDD.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<Integer, int[]>>, LocalSearchSubTask>() {
                    @Override
                    public Iterator<LocalSearchSubTask> call(Iterator<Tuple2<Integer, int[]>> iter) throws Exception {
                        List<LocalSearchSubTask> tasksList = new ArrayList<>();
                        MatchesCollectorInterface collector;
                        String outputPath = jobProp.getProperty(MyConf.OUTPUT_FILE_PATH);
                        boolean output = !outputPath.equalsIgnoreCase("null");
                        if (output) {
                            collector = new OutputCollector(outputPath + "/" +
                                    jobProp.getProperty(MyConf.TIMESTAMP) + "/partition-" + TaskContext.getPartitionId());
                        } else {
                            collector = new CountCollector();
                        }
                        while (iter.hasNext()) {
                            Tuple2<Integer, int[]> vertexToAdj = iter.next();
                            int vid = vertexToAdj._1();
                            int[] deltaAdj = vertexToAdj._2();
                            LocalSearchTask task = new LocalSearchTask(vid, deltaAdj, -1);
                            IncrementalExecutionPlan[] plans = broadcastPlans.value();
                            for (int i = 0; i < plans.length; i++) {
                                LocalSearchSubTask subtask = new LocalSearchSubTask(plans[i], jobProp);
                                subtask.resetTask(vid, task, 0);
                                tasksList.add(subtask);
                            }
                            deltaVertexNumAccu.add(1L);
                        }
                        return tasksList.iterator();
                    }
                }).repartition(partitionNum);

        JavaRDD <LocalSearchSubTask> roundTwoRDD = tasksRDD.mapPartitions(
                    new FlatMapFunction<Iterator<LocalSearchSubTask>, LocalSearchSubTask>() {
                        @Override
                        public Iterator<LocalSearchSubTask> call(Iterator<LocalSearchSubTask> iter) throws Exception {
                            //String driverHostname = jobProp.getProperty(MyConf.DRIVER_HOSTNAME);
                            //ClusterStatusClient statusClient = ClusterStatusClient.getExecutorLevelClient(driverHostname);
                            //statusClient.startActivePartition();
                            IncrementalExecutionPlan[] plans = broadcastPlans.value();
                            //int patternEdgeNum = Integer.parseInt(jobProp.getProperty(MyConf.PATTERN_EDGE_NUM));
                            AbstractExecutorService threadPool = ProcessLevelThreadPool.pool(numWorkingThreads);
                            GraphWithTimestampDBClient client = GraphWithTimestampDBClient.getProcessLevelStorage();
                            MatchesCollectorInterface collector;
                            String outputPath = jobProp.getProperty(MyConf.OUTPUT_FILE_PATH);
                            boolean output = !outputPath.equalsIgnoreCase("null");
                            if (output) {
                                collector = new OutputCollector(outputPath + "/" +
                                        jobProp.getProperty(MyConf.TIMESTAMP) + "/partition-" + TaskContext.getPartitionId());
                            } else {
                                collector = new CountCollector();
                            }
                            //List<VertexTaskStats> stats = new ArrayList<>();
                            Queue<Future<TaskExecutionReport>> futures = new ConcurrentLinkedQueue<>();
                            while (iter.hasNext()) {
                                final LocalSearchSubTask subTask = iter.next();
                                Future<TaskExecutionReport> future = threadPool.submit(() -> {
                                    try {
                                        TaskExecutionReport report =
                                                subTask.execute(client, collector, numWorkingThreads, 1);
                                        return report;
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        throw new RuntimeException(e.toString());
                                    }
                                });
                                futures.add(future);
                            }
                            List<LocalSearchSubTask> newTasks = new ArrayList<>();
                            for (Future<TaskExecutionReport> future: futures) {
                                vertexTaskNumAccu.add(1L);
                                TaskExecutionReport report = future.get();
                                newTasks.addAll(report.getGeneratedSubTasks());
                                executionTimeAccu.add(report.getStatus().getExecutionTime());
                                deltaEmbeddingNumAccu.add(report.getStatus().getEnumerateResultsCount());
                                numDBQExecutedAccu.add(report.getStatus().dbqCount);
                                numINTExecutedAccu.add(report.getStatus().intCount);
                                numENUExecutedAccu.add(report.getStatus().enuCount);
                            }
                            return newTasks.iterator();
                            /*
                            Stream<LocalSearchSubTask> taskStream = StreamSupport.stream(
                                    Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
                            Stream <LocalSearchSubTask> newTaskStream  = taskStream.flatMap(task -> {
                                try {
                                    TaskExecutionReport report = task.execute(client, collector, numCores, 1);
                                    deltaEmbeddingNumAccu.add(report.getStatus().getEnumerateResultsCount());
                                    vertexTaskNumAccu.add(1L);
                                    executionTimeAccu.add(report.getStatus().getExecutionTime());
                                    return report.getGeneratedSubTasks().stream();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    throw new RuntimeException(e);
                                }
                            });
                            return newTaskStream.iterator();
                             */
                        }
                    }).repartition(partitionNum);
       JavaRDD<VertexTaskStats> statsRDD = roundTwoRDD.mapPartitions(iter -> {
            IncrementalExecutionPlan[] plans = broadcastPlans.value();
            //int patternEdgeNum = Integer.parseInt(jobProp.getProperty(MyConf.PATTERN_EDGE_NUM));
            ForkJoinPool threadPool = ProcessLevelThreadPool.pool(numWorkingThreads);
            GraphWithTimestampDBClient client = GraphWithTimestampDBClient.getProcessLevelStorage();
            MatchesCollectorInterface collector;
            String outputPath = jobProp.getProperty(MyConf.OUTPUT_FILE_PATH);
            boolean output = !outputPath.equalsIgnoreCase("null");
            if (output) {
                collector = new OutputCollector(outputPath + "/" +
                        jobProp.getProperty(MyConf.TIMESTAMP) + "/partition-" + TaskContext.getPartitionId());
            } else {
                collector = new CountCollector();
            }
            /*
            Deque<LocalSearchSubTask> taskQueue = new LinkedList<>();
            ArrayList<VertexTaskStats> stats = new ArrayList<>();
            while (iter.hasNext() || !taskQueue.isEmpty()) {
                LocalSearchSubTask task = iter.hasNext() ? iter.next() : taskQueue.pollLast();
                TaskExecutionReport report = task.execute(client, collector, threadPool.getParallelism(), 2);
                stats.add(report.getStatus());
                vertexTaskNumAccu.add(1L);
                executionTimeAccu.add(report.getStatus().getExecutionTime());
                deltaEmbeddingNumAccu.add(report.getStatus().getEnumerateResultsCount());
                for(LocalSearchSubTask newTask: report.getGeneratedSubTasks())
                        taskQueue.offerLast(newTask);
            }
             */

           /*
            Queue<EnumerateForkJoinTask> taskQueue = new ConcurrentLinkedQueue<>();
            while (iter.hasNext()) {
                LocalSearchSubTask task = iter.next();
                EnumerateForkJoinTask joinTask = new EnumerateForkJoinTask(client, collector, 2, task, numCores);
                threadPool.execute(joinTask);
                taskQueue.add(joinTask);
            }
            ArrayList<VertexTaskStats> stats = new ArrayList<>();
            while (!taskQueue.isEmpty()) {
                EnumerateForkJoinTask task = taskQueue.poll();
                List<VertexTaskStats> localStats = task.join();
                stats.addAll(localStats);
            }
            vertexTaskNumAccu.add(stats.size());
            for (VertexTaskStats stat: stats) {
                deltaEmbeddingNumAccu.add(stat.getEnumerateResultsCount());
                executionTimeAccu.add(stat.getExecutionTime());
            }
            */
           Queue<Future<TaskExecutionReport>> futures = new ConcurrentLinkedQueue<>();
           ArrayList<VertexTaskStats> stats = new ArrayList<>();
           while (iter.hasNext()) {
               LocalSearchSubTask task = iter.next();
               Future<TaskExecutionReport> future = threadPool.submit(() -> {
                   try {
                       TaskExecutionReport report = task.execute(client, collector, numWorkingThreads, 2);
                       return report;
                   } catch (Exception e) {
                       e.printStackTrace();
                       throw new RuntimeException(e);
                   }
               });
               futures.add(future);
           }
           while (!futures.isEmpty()) {
               Future<TaskExecutionReport> future = futures.poll();
               TaskExecutionReport report = future.get();
               vertexTaskNumAccu.add(1L);
               deltaEmbeddingNumAccu.add(report.getStatus().getEnumerateResultsCount());
               executionTimeAccu.add(report.getStatus().getExecutionTime());
               numDBQExecutedAccu.add(report.getStatus().dbqCount);
               numINTExecutedAccu.add(report.getStatus().intCount);
               numENUExecutedAccu.add(report.getStatus().enuCount);
               stats.add(report.getStatus());
               for (LocalSearchSubTask newTask: report.getGeneratedSubTasks()) {
                   Future<TaskExecutionReport> newFuture = threadPool.submit(() -> {
                       try {
                           TaskExecutionReport localReport = newTask.execute(client, collector, numWorkingThreads, 2);
                           return localReport;
                       } catch (Exception e) {
                           e.printStackTrace();
                           throw new RuntimeException(e);
                       }
                   });
                   futures.add(newFuture);
               }
           }
           collector.cleanup();
            return stats.iterator();
        });

       statsRDD.count();


        /*
        String statsOutputPath = jobProp.getProperty(MyConf.TASK_STATS_OUTPUT_FILE_PATH);
        boolean outputStats = !statsOutputPath.equalsIgnoreCase("null");

        if (outputStats) {
            taskStatsRDD.saveAsTextFile(statsOutputPath + "/timestep-" + jobProp.getProperty(MyConf.TIMESTAMP));
        } else {
            taskStatsRDD.count(); // do an action operation
        }
//        taskStatsRDD.count(); // do an action operation
         */

        System.err.println("Delta embeddings num: " + deltaEmbeddingNumAccu.value());
        //System.err.println("Counteract embeddings num: " + counteractEmbeddingNumAccu.value());
        System.err.println("Delta vertex num: " + deltaVertexNumAccu.value());
        System.err.println("Vertex task num: " + vertexTaskNumAccu.value());
        System.err.println("Serial Execution Time(ns): " + executionTimeAccu.value());
        System.err.println("Number of executed DBQ instructions: " + numDBQExecutedAccu.value());
        System.err.println("Number of executed INT instructions: " + numINTExecutedAccu.value());
        System.err.println("Number of executed ENU instructions: " + numENUExecutedAccu.value());

        return deltaEmbeddingNumAccu.value();
    }

    public static class EnumerateForkJoinTask extends RecursiveTask<List<VertexTaskStats>> {
        private GraphWithTimestampDBClient client;
        private MatchesCollectorInterface collector;
        private int numRound;
        private LocalSearchSubTask task;
        private int numThread;

        public EnumerateForkJoinTask(GraphWithTimestampDBClient client, MatchesCollectorInterface collector,
                                     int numRound, LocalSearchSubTask task, int numThread) {
            this.client = client;
            this.collector = collector;
            this.numRound = numRound;
            this.task = task;
            this.numThread = numThread;
        }

        @Override
        protected List<VertexTaskStats> compute() {
            List<VertexTaskStats> stats = new ArrayList<>();
            try {
                TaskExecutionReport report = task.execute(client, collector, numThread, numRound);
                stats.add(report.getStatus());
                report.getGeneratedSubTasks().stream().map(newTask ->
                   new EnumerateForkJoinTask(client, collector, numRound, newTask, numThread).fork())
                .flatMap(t -> t.join().stream()).forEach(stat -> stats.add(stat));
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e.toString());
            }
            return stats;
        }
    }


    public static String[] loadExecutionPlan(File file) {
        List<String> execPlan = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = null;
            //System.out.println(filePath);
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                execPlan.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(104);
        };
        return execPlan.toArray(new String[0]);
    }

    public static String generateCommaSeparatedString(int arr[]) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < arr.length; i++) {
            builder.append("," + arr[i]);
        }
        return builder.substring(1);
    }
}
