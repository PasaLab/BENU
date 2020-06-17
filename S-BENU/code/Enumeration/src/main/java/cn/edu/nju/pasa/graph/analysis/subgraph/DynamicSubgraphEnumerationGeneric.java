package cn.edu.nju.pasa.graph.analysis.subgraph;

import cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.CountCollector;
import cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.Enumerator;
import cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.MatchesCollectorInterface;
import cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.OutputCollector;
import cn.edu.nju.pasa.graph.analysis.subgraph.dynamic.CommonFunctions;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.analysis.subgraph.util.CommandLineUtils;
import cn.edu.nju.pasa.graph.storage.multiversion.GraphWithTimestampDBClient;
import cn.edu.nju.pasa.graph.util.HDFSUtil;
import cn.edu.nju.pasa.graph.util.ProcessLevelThreadPool;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public class DynamicSubgraphEnumerationGeneric {

    private static Partitioner globalPartitioner;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("Dynamic Subgraph Enumeration - Generic")
                .set("spark.ui.showConsoleProgress", "false");
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        JavaSparkContext javasc = new JavaSparkContext(conf);


        Properties jobProp = CommandLineUtils.parseArgs(args);
        String initialForwardGraphPath = jobProp.getProperty(MyConf.INITIAL_FORWARD_GRAPH_PATH);
        String initialReverseGraphPath = jobProp.getProperty(MyConf.INITIAL_REVERSE_GRAPH_PATH);
        String updateEdgesPath = jobProp.getProperty(MyConf.UPDATE_EDGES_PATH);
        int updateEdgesBatch = Integer.parseInt(jobProp.getProperty(MyConf.UPDATE_EDGES_BATCH));
        int parallelism = Integer.parseInt(jobProp.getProperty(MyConf.PARTITION_NUM));
        int batchSize = Integer.parseInt(jobProp.getProperty(MyConf.BLOCKING_QUEUE_SIZE));

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
        writeInitGraphToDB(initAdjsRDD, batchSize);

        long t0 = System.nanoTime();
        System.err.println("Initial storage elapsed time (s): " + (t0 - start)/1e9);

        double initialStorageTime = (t0 - start)/1e9;
        double updateStorageTime = 0;
        double enumerationTime = 0;
        double mergeUpdateTime = 0;

        System.err.println("Load executors...");
        //String patternGraphName = jobProp.getProperty(MyConf.PATTERN_GRAPH_NAME);
        //int patternEdgeNum = Integer.parseInt(jobProp.getProperty(MyConf.PATTERN_EDGE_NUM));
        String incrementalExecPlansPath = jobProp.getProperty(MyConf.INCREMENTAL_EXEC_PLANS_PATH);
        File file = new File(incrementalExecPlansPath);
        File[] fs = file.listFiles();
        Arrays.sort(fs, (a, b) -> a.getName().compareTo(b.getName()));
        Enumerator[] enumerators = new Enumerator[fs.length];
        for(int i = 0; i < fs.length; i++) {
            File f = fs[i];
            System.err.println("execPlan: " + f.getPath());
            String[] execPlan = loadExecutionPlan(f);
            //String[] execPlan = loadExecutionPlan("execplan/dynamic/" + patternGraphName + "/" + (i+1) + ".txt");
            //String[] execPlan = loadExecutionPlan("execplan/dynamic/o1/" + patternGraphName + "/" + (i+1) + ".txt");
            //String[] execPlan = loadExecutionPlan("execplan/dynamic/raw/" + patternGraphName + "/" + (i+1) + ".txt");

            Enumerator enumerator = new Enumerator(execPlan.length);
            enumerator.construct(execPlan);
            enumerators[i] = enumerator;
        }

        Broadcast<Enumerator[]> broadcastEnumerators = javasc.broadcast(enumerators);
        /*
        LongAccumulator[] intermidiateResultsAccus = new LongAccumulator[8*5];
        for(int i = 0; i < intermidiateResultsAccus.length; i++) {
            intermidiateResultsAccus[i] = javasc.sc().longAccumulator("task" + (i / 5) + ".f" + (i % 5));
        }

         */

        Long totalDeltaEmbeddingNum = 0L;

        Path updatesFilePath = new Path(updateEdgesPath);
        try {
            for (Path filePath : HDFSUtil.listFiles(updatesFilePath.toString())) {
                logicalTimestamp++;
                if(logicalTimestamp > updateEdgesBatch) break;
                jobProp.setProperty(MyConf.TIMESTAMP, String.valueOf(logicalTimestamp));
                t0 = System.nanoTime();

                System.err.println("---------------- Timestamp " + logicalTimestamp + "----------------");
                System.err.println("update edges path: " + updateEdgesPath + "/" + filePath.getName());

                System.err.println("Store the update edges...");
                // update edge file : src dst 1/-1
                JavaPairRDD<Integer, Integer> deltaForwardEdgeRDD = readEdgeFileToForwardEdges(javasc,
                        updateEdgesPath + "/" + filePath.getName(), parallelism);
                JavaPairRDD<Integer, int[]> deltaForwardAdjRDD = edgesToAdjs(deltaForwardEdgeRDD);
                deltaForwardAdjRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
                // edge file: src dst

                JavaPairRDD<Integer, Integer> deltaReverseEdgeRDD = readEdgeFileToReverseEdges(javasc,
                        updateEdgesPath + "/" + filePath.getName(), parallelism);
                JavaPairRDD<Integer, int[]> deltaReverseAdjRDD = edgesToAdjs(deltaReverseEdgeRDD);

                JavaPairRDD<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> deltaAdjsRDD = deltaForwardAdjRDD
                        .fullOuterJoin(deltaReverseAdjRDD);
                deltaAdjsRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
                writeDeltaEdgesToDB(deltaAdjsRDD, batchSize, logicalTimestamp);

                long t1 = System.nanoTime();
                //System.err.println("Store the update edges time (s): " + (t1 - t0)/1e9);
                updateStorageTime = updateStorageTime + (t1 - t0)/1e9;

                System.err.println("Enumerate...");

                totalDeltaEmbeddingNum += enumerate(javasc, deltaForwardAdjRDD, jobProp, broadcastEnumerators);

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
        System.exit(0);
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

    public static JavaPairRDD<Integer, Integer> readEdgeFileToForwardEdges(JavaSparkContext javasc, String graphFilePath, int parallelism) {
        JavaRDD<String> edgeFile = javasc.textFile(graphFilePath, parallelism);
        JavaPairRDD<Integer, Integer> edges = edgeFile.mapToPair(line ->  {
            String[] fields = line.split(" ");
            int src = Integer.parseInt(fields[0]);
            int dst = Integer.parseInt(fields[1]);
            int w = Integer.parseInt(fields[2]);
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
                                          int batchSize) {
        initAdjsRDD.foreachPartition(partition -> {
            GraphWithTimestampDBClient graphStorage = GraphWithTimestampDBClient.getProcessLevelStorage();
            Int2ObjectMap<int[][]> records = new Int2ObjectOpenHashMap<>();
            while (partition.hasNext()) {
                Tuple2<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> record = partition.next();
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
    }

    public static void writeDeltaEdgesToDB(JavaPairRDD<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> deltaAdjRDD,
                                           int batchSize, int T) {
        deltaAdjRDD.foreachPartition(partition -> {
            GraphWithTimestampDBClient graphStorage = GraphWithTimestampDBClient.getProcessLevelStorage();
            graphStorage.clearCache();
            Int2ObjectMap<int[][]> deltaAdjs = new Int2ObjectOpenHashMap<>();
            try {
                while (partition.hasNext()) {
                    Tuple2<Integer, Tuple2<Optional<int[]>, Optional<int[]>>> record = partition.next();
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
                                 Broadcast<Enumerator[]> broadcastEnumerators) throws Exception {

        LongAccumulator deltaEmbeddingNumAccu = javasc.sc().longAccumulator("delta.embeddings.num");
        //LongAccumulator counteractEmbeddingNumAccu = javasc.sc().longAccumulator("counteract.embeddings.num");
        LongAccumulator deltaVertexNumAccu = javasc.sc().longAccumulator("delta.vertex.num");
        LongAccumulator vertexTaskNumAccu = javasc.sc().longAccumulator("vertex.task.num");
        LongAccumulator executionTimeAccu = javasc.sc().longAccumulator("serialExecutionTime(ns)");
        //LongAccumulator deltaEdgeNumAccu = javasc.sc().longAccumulator("delta.edge.num");
        int partitionNum = Integer.parseInt(jobProp.getProperty(MyConf.PARTITION_NUM));

        JavaRDD<VertexCentricAnalysisTask> tasksRDD = updateVertexToAdjRDD.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<Integer, int[]>>, VertexCentricAnalysisTask>() {
                    @Override
                    public Iterator<VertexCentricAnalysisTask> call(Iterator<Tuple2<Integer, int[]>> iter) throws Exception {
                        boolean enableLoadBalance = Boolean.parseBoolean(jobProp.getProperty(MyConf.ENABLE_LOAD_BALANCE));
                        int threshold = Integer.parseInt(jobProp.getProperty(MyConf.LOAD_BALANCE_THRESHOLD));
                        List<VertexCentricAnalysisTask> tasksList = new ArrayList<>();

                        while (iter.hasNext()) {
                            Tuple2<Integer, int[]> vertexToAdj = iter.next();
                            int vid = vertexToAdj._1();
                            int[] deltaAdj = vertexToAdj._2();
                            int taskNum = (deltaAdj.length - 1) / threshold + 1;
                            if (enableLoadBalance && taskNum > 1) { // task splitting
                                int balanceAdj[] = new int[deltaAdj.length];
                                int avg = deltaAdj.length / taskNum;
                                int reminder = deltaAdj.length % taskNum;
                                //reorder adj
                                for (int i = 0; i < deltaAdj.length; i++) {
                                    int taskId = i % taskNum;
                                    int m = taskId * avg;
                                    int n = i / taskNum;
                                    int k = Math.min(taskId, reminder);
                                    balanceAdj[m + n + k] = deltaAdj[i];
                                }
                                int start = -1;
                                int end = -1;
                                for (int n = 0; n < taskNum; n++) {
                                    int k = (n + 1) <= reminder ? 1 : 0;
                                    start = end + 1;
                                    end = start + avg - 1 + k;
                                    VertexCentricAnalysisTask task = new VertexCentricAnalysisTask(vid, balanceAdj, start, end);
                                    tasksList.add(task);
                                }
                            } else {
                                VertexCentricAnalysisTask task = new VertexCentricAnalysisTask(vid, deltaAdj, 0, deltaAdj.length-1);
                                tasksList.add(task);
                            }
                            deltaVertexNumAccu.add(1L);
                        }
                        return tasksList.iterator();
                    }
                }).repartition(partitionNum);

        JavaRDD <VertexTaskStats> taskStatsRDD = tasksRDD.mapPartitions(
                new FlatMapFunction<Iterator<VertexCentricAnalysisTask>, VertexTaskStats>() {
                    @Override
                    public Iterator<VertexTaskStats> call(Iterator<VertexCentricAnalysisTask> iter) throws Exception {
                        Enumerator[] enumerators = broadcastEnumerators.value();
                        //int patternEdgeNum = Integer.parseInt(jobProp.getProperty(MyConf.PATTERN_EDGE_NUM));
                        int numCores = Runtime.getRuntime().availableProcessors();
                        ForkJoinPool threadPool = ProcessLevelThreadPool.pool(numCores);

                        List<Future<VertexTaskStats>> futures = new ArrayList<>();
                        List<VertexTaskStats> stats = new ArrayList<>();

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
                            VertexCentricAnalysisTask task = iter.next();
                            Enumerator[] clonedEnumerators = new Enumerator[enumerators.length];
                            for (int i = 0; i < enumerators.length; i++) {
                                Enumerator clonedEnumerator = Enumerator.newInstance(enumerators[i]);
                                clonedEnumerator.prepare(jobProp);
                                clonedEnumerators[i] = clonedEnumerator;
                            }
                            for (int i = 0; i < enumerators.length; i++) {
                                final int executionPlanIndex = i;
                                Future<VertexTaskStats> future = threadPool.submit(() -> {
                                    VertexTaskStats stat = clonedEnumerators[executionPlanIndex].executeVertexTask(task.getVid(), task, collector);
                                    return stat;
                                });
                                futures.add(future);
                            }
                        }


                        for (Future<VertexTaskStats> f : futures) {
                            VertexTaskStats s = f.get();

                            if (s.getExecutionTime() > 1e10)
                                System.out.println(String.format("%s: slow task: v=%d, delta deg=%d, splitted deg=%d, time=%f s",
                                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()),
                                        s.getVid(), s.getDegree(), s.getTwoHopNeighborEdgeNum(), s.getExecutionTime() / 1e9));


                            vertexTaskNumAccu.add(1L);
                            executionTimeAccu.add(s.getExecutionTime());
                            stats.add(s);
                        }
                        deltaEmbeddingNumAccu.add(collector.getDeltaCount());
                        collector.cleanup();
                        return stats.iterator();
                    }
                });
        /*
        String outputPath = jobProp.getProperty(MyConf.OUTPUT_FILE_PATH);
        boolean output = !outputPath.equalsIgnoreCase("null");

        if (output) {
            taskStatsRDD.saveAsTextFile(outputPath + "/" + jobProp.getProperty(MyConf.TIMESTAMP));
        } else {
            taskStatsRDD.count(); // do an action operation
        }
         */
        taskStatsRDD.count(); // do an action operation

        System.err.println("Delta embeddings num: " + deltaEmbeddingNumAccu.value());
        //System.err.println("Counteract embeddings num: " + counteractEmbeddingNumAccu.value());
        System.err.println("Delta vertex num: " + deltaVertexNumAccu.value());
        System.err.println("Vertex task num: " + vertexTaskNumAccu.value());
        System.err.println("Serial Execution Time(ns): " + executionTimeAccu.value());

        return deltaEmbeddingNumAccu.value();
    }

    public static Long[] enumerate(JavaSparkContext javasc,
                                   JavaPairRDD<Integer, int[]> updateVertexToAdjRDD,
                                   Properties jobProp,
                                   Broadcast<Enumerator[]> broadcastEnumerators,
                                   LongAccumulator[] intermidiateResultsAccus) throws Exception {

        LongAccumulator deltaEmbeddingNumAccu = javasc.sc().longAccumulator("delta.embeddings.num");
        LongAccumulator counteractEmbeddingNumAccu = javasc.sc().longAccumulator("counteract.embeddings.num");
        LongAccumulator deltaVertexNumAccu = javasc.sc().longAccumulator("delta.vertex.num");
        LongAccumulator vertexTaskNumAccu = javasc.sc().longAccumulator("vertex.task.num");
        LongAccumulator executionTimeAccu = javasc.sc().longAccumulator("serialExecutionTime(ns)");
        //LongAccumulator deltaEdgeNumAccu = javasc.sc().longAccumulator("delta.edge.num");

        JavaRDD<VertexTaskStats> taskStatsRDD = updateVertexToAdjRDD.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<Integer, int[]>>, VertexTaskStats>() {
                    @Override
                    public Iterator<VertexTaskStats> call(Iterator<Tuple2<Integer, int[]>> iter) throws Exception {
                        Enumerator[] enumerators = broadcastEnumerators.value();
                        //int patternEdgeNum = Integer.parseInt(jobProp.getProperty(MyConf.PATTERN_EDGE_NUM));

                        Enumerator[] clonedEnumerators = new Enumerator[enumerators.length];
                        for (int i = 0; i < enumerators.length; i++) {
                            Enumerator clonedEnumerator = Enumerator.newInstance(enumerators[i]);
                            clonedEnumerator.prepare(jobProp);
                            clonedEnumerators[i] = clonedEnumerator;
                        }

                        List<VertexTaskStats> stats = new ArrayList<>();

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
                            VertexCentricAnalysisTask task = new VertexCentricAnalysisTask(vid, deltaAdj);
                            for (int i = 0; i < enumerators.length; i++) {
                                //VertexTaskStats stat = clonedEnumerators[i].executeVertexTask(vid, task, collector);
                                VertexTaskStats stat = clonedEnumerators[i].executeVertexTask(vid, task, collector,
                                        i, intermidiateResultsAccus);
                                vertexTaskNumAccu.add(1L);
                                executionTimeAccu.add(stat.getExecutionTime());
                                //deltaEdgeNumAccu.add(stat.getDegree());
                                stats.add(stat);
                            }
                            deltaVertexNumAccu.add(1L);
                        }

                        deltaEmbeddingNumAccu.add(collector.getDeltaCount());
                        counteractEmbeddingNumAccu.add(collector.getCounteractCount());
                        collector.cleanup();
                        return stats.iterator();
                    }
                }, true);
        taskStatsRDD.count(); // do an action operation

        System.err.println("Delta embeddings num: " + deltaEmbeddingNumAccu.value());
        System.err.println("Counteract embeddings num: " + counteractEmbeddingNumAccu.value());
        System.err.println("Delta vertex num: " + deltaVertexNumAccu.value());
        System.err.println("Vertex task num: " + vertexTaskNumAccu.value());
        System.err.println("Enumeration execution time: " + executionTimeAccu.value());

        return new Long[]{deltaEmbeddingNumAccu.value(), counteractEmbeddingNumAccu.value()};
    }

    public static String[] loadExecutionPlan(String filePath) throws IOException {
        List<String> execPlan = new ArrayList<>();
        InputStream inputStream = DynamicSubgraphEnumerationGeneric.class.getClassLoader().getResourceAsStream(filePath);
        try(BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = null;
            //System.out.println(filePath);
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                execPlan.add(line);
            }
        };
        return execPlan.toArray(new String[0]);
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
}
