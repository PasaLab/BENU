package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.analysis.subgraph.DynamicSubgraphEnumerationGeneric2;
import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.storage.multiversion.AdjType;
import cn.edu.nju.pasa.graph.storage.multiversion.GraphWithTimestampDBClient;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestLocalSearchSubTask {
    @Mock
    GraphWithTimestampDBClient client;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    public Map<Integer, int[]> loadAdjFile(String path) throws FileNotFoundException {
        Map<Integer, int[]> adjMap = new HashMap<>();
        int maxvid = -1;
        Scanner scanner = new Scanner(new File(path));
        while (scanner.hasNextLine()) {
            String line  = scanner.nextLine();
            String f[] = line.split(" ");
            int adj[] = new int[f.length - 1];
            int vid = Integer.parseInt(f[0]);
            if (vid > maxvid) maxvid = vid;
            for (int i = 1; i < f.length; i++) {
                int nvid = Integer.parseInt(f[i]);
                if (nvid > maxvid)
                    maxvid = nvid;
                adj[i-1] = nvid;
            }
            adjMap.put(vid, adj);
        }
        scanner.close();
        for (int i = 0; i <= maxvid; i++) {
            if (!adjMap.containsKey(i))
                adjMap.put(i, new int[0]);
        }
        return adjMap;
    }

    public Map<Integer, int[]> loadUpdateEdgeForwardList(String file) throws IOException {
        Map<Integer, int[]> deltaAdjMap = new HashMap<>();
        ArrayList<Tuple2<Integer, Integer>> deltaEdges = new ArrayList<>();
        Scanner scanner = new Scanner(new File(file));
        int maxvid = -1;
        while (scanner.hasNextLine()) {
            String f[] = scanner.nextLine().split(" ");
            int src = Integer.parseInt(f[0]);
            int dst = Integer.parseInt(f[1]);
            maxvid = Math.max(src, maxvid);
            maxvid = Math.max(dst, maxvid);
        }
        scanner.close();
        scanner = new Scanner(new File(file));
        while (scanner.hasNextLine()) {
            String f[] = scanner.nextLine().split(" ");
            int src = Integer.parseInt(f[0]);
            int dst = Integer.parseInt(f[1]);
            int op = Integer.parseInt(f[2]);
            if (op == 1) {
                deltaEdges.add(new Tuple2<>(src, dst));
            }
            if (op == -1) {
                dst = CommonFunctions.setSignBitTo1(dst);
                deltaEdges.add(new Tuple2<>(src , dst));
            }
        }
        scanner.close();
        deltaEdges.stream()
                .collect(Collectors.groupingBy(t -> t._1))
                .entrySet().stream().forEach(entry -> {
            int vid = entry.getKey();
            List<Integer> adjList = entry.getValue().stream().map(t -> t._2).collect(Collectors.toList());
            Collections.sort(adjList, (a,b) -> CommonFunctions.setSignBitTo0(a) - CommonFunctions.setSignBitTo0(b));
            int adj[] = new int[adjList.size()];
            for (int i = 0; i < adj.length; i++) {
                adj[i] = adjList.get(i);
            }
            deltaAdjMap.put(vid, adj);
        });
        for (int i = 0; i <= maxvid; i++) {
            if (!deltaAdjMap.containsKey(i))
                deltaAdjMap.put(i, new int[0]);
        }
        return deltaAdjMap;
    }

    public Map<Integer, int[]> loadUpdateEdgeReverseList(String file) throws IOException {
        Map<Integer, int[]> deltaAdjMap = new HashMap<>();
        ArrayList<Tuple2<Integer, Integer>> deltaEdges = new ArrayList<>();
        Scanner scanner = new Scanner(new File(file));
        int maxvid = -1;
        while (scanner.hasNextLine()) {
            String f[] = scanner.nextLine().split(" ");
            int src = Integer.parseInt(f[0]);
            int dst = Integer.parseInt(f[1]);
            maxvid = Math.max(src, maxvid);
            maxvid = Math.max(dst, maxvid);
        }
        scanner.close();
        scanner = new Scanner(new File(file));
        while (scanner.hasNextLine()) {
            String f[] = scanner.nextLine().split(" ");
            int src = Integer.parseInt(f[0]);
            int dst = Integer.parseInt(f[1]);
            int op = Integer.parseInt(f[2]);
            if (op == 1) {
                deltaEdges.add(new Tuple2<>(dst, src));
            }
            if (op == -1) {
                src = CommonFunctions.setSignBitTo1(src);
                deltaEdges.add(new Tuple2<>(dst , src));
            }
        }
        scanner.close();
        deltaEdges.stream()
                .collect(Collectors.groupingBy(t -> t._1))
                .entrySet().stream().forEach(entry -> {
            int vid = entry.getKey();
            List<Integer> adjList = entry.getValue().stream().map(t -> t._2).collect(Collectors.toList());
            Collections.sort(adjList, (a,b) -> CommonFunctions.setSignBitTo0(a) - CommonFunctions.setSignBitTo0(b));
            int adj[] = new int[adjList.size()];
            for (int i = 0; i < adj.length; i++) {
                adj[i] = adjList.get(i);
            }
            deltaAdjMap.put(vid, adj);
        });
        for (int i = 0; i <= maxvid; i++) {
            if (!deltaAdjMap.containsKey(i))
                deltaAdjMap.put(i, new int[0]);
        }
        return deltaAdjMap;
    }


    @Test
    public void testLoadUpdateListFile() throws IOException {
        Map<Integer, int[]> deltaAdjMap = loadUpdateEdgeForwardList("src/test/resources/test_update_edge_list.txt");
        int adj1[] = {2, CommonFunctions.setSignBitTo1(3), 4};
        int adj1F1[] = {2,3,4};
        int adj1F2[] = {2,4,CommonFunctions.setSignBitTo1(3)};
        int adj2[] = {1, 3};
        int adj3[] = {CommonFunctions.setSignBitTo1(2)};
        assertArrayEquals(deltaAdjMap.get(1), adj1);
        assertArrayEquals(deltaAdjMap.get(2), adj2);
        assertArrayEquals(deltaAdjMap.get(3), adj3);
        assertFalse(Arrays.equals(deltaAdjMap.get(1), adj1F1));
        assertFalse(Arrays.equals(deltaAdjMap.get(1), adj1F2));
        assertTrue(deltaAdjMap.get(4).length == 0);
        assertTrue(deltaAdjMap.get(0).length == 0);
        assertNull(deltaAdjMap.get(5));
        Map<Integer, int[]> deltaReverseAdjMap = loadUpdateEdgeReverseList("src/test/resources/test_update_edge_list.txt");
        int radj1[] = {2};
        int radj2[] = {1, CommonFunctions.setSignBitTo1(3)};
        assertArrayEquals(deltaReverseAdjMap.get(1), radj1);
        assertArrayEquals(deltaReverseAdjMap.get(2), radj2);
    }

    public void prepareDatabase(Map<Integer, int[]> initForwardAdj, Map<Integer, int[]> initReverseAdj,
                                Map<Integer, int[]> deltaForwardAdj, Map<Integer, int[]> deltaReverseAdj)
            throws Exception {
        reset(client);
        int empty[] = new int[0];
        for (int vid : initForwardAdj.keySet()) {
            IntArrayList oldForwardAdjList = IntArrayList.wrap(initForwardAdj.get(vid));
            IntSet oldSet = new IntArraySet(oldForwardAdjList);
            IntSet unaltered = new IntArraySet(oldSet);
            IntSet newSet = new IntArraySet(oldForwardAdjList);
            for (int x : deltaForwardAdj.getOrDefault(vid, empty)) {
                if (CommonFunctions.isSignBit1(x)) {
                    unaltered.remove(x);
                    newSet.remove(x);
                } else {
                    newSet.add(x);
                }
            }
            int unalteredArr[] = unaltered.toIntArray();
            Arrays.sort(unalteredArr);
            int newArr[] = newSet.toIntArray();
            Arrays.sort(newArr);
            when(client.get(vid, AdjType.UNALTERED, AdjType.FORWARD, true, 0)).thenReturn(unaltered.toIntArray());
            when(client.get(vid, AdjType.UNALTERED, AdjType.FORWARD, false, 0)).thenReturn(unaltered.toIntArray());
            when(client.get(vid, AdjType.EITHER, AdjType.FORWARD, true, 0)).thenReturn(newArr);
            when(client.get(vid, AdjType.EITHER, AdjType.FORWARD, false, 0)).thenReturn(oldForwardAdjList.toIntArray());
            when(client.get(vid, AdjType.DELTA, AdjType.FORWARD, true, 0)).thenReturn(deltaForwardAdj.get(vid));
            when(client.get(vid, AdjType.DELTA, AdjType.FORWARD, false, 0)).thenReturn(deltaForwardAdj.get(vid));
        }
        for (int vid : initReverseAdj.keySet()) {
            IntArrayList oldReverseAdjList = IntArrayList.wrap(initReverseAdj.get(vid));
            IntSet oldSet = new IntArraySet(oldReverseAdjList);
            IntSet unaltered = new IntArraySet(oldSet);
            IntSet newSet = new IntArraySet(oldReverseAdjList);
            for (int x : deltaReverseAdj.getOrDefault(vid, empty)) {
                if (CommonFunctions.isSignBit1(x)) {
                    unaltered.remove(x);
                    newSet.remove(x);
                } else {
                    newSet.add(x);
                }
            }
            int unalteredArr[] = unaltered.toIntArray();
            Arrays.sort(unalteredArr);
            int newArr[] = newSet.toIntArray();
            Arrays.sort(newArr);
            when(client.get(vid, AdjType.UNALTERED, AdjType.REVERSE, true, 0)).thenReturn(unaltered.toIntArray());
            when(client.get(vid, AdjType.UNALTERED, AdjType.REVERSE, false, 0)).thenReturn(unaltered.toIntArray());
            when(client.get(vid, AdjType.EITHER, AdjType.REVERSE, true, 0)).thenReturn(newArr);
            when(client.get(vid, AdjType.EITHER, AdjType.REVERSE, false, 0)).thenReturn(oldReverseAdjList.toIntArray());
            when(client.get(vid, AdjType.DELTA, AdjType.REVERSE, true, 0)).thenReturn(deltaForwardAdj.get(vid));
            when(client.get(vid, AdjType.DELTA, AdjType.REVERSE, false, 0)).thenReturn(deltaForwardAdj.get(vid));
        }
    }

    public void prepareDatabase(String initForwardAdjFilePath, String initReverseAdjFilePath, String updateFilePath) throws Exception {
        Map<Integer, int[]> initForwardAdj = loadAdjFile(initForwardAdjFilePath);
        Map<Integer, int[]> initReverseAdj = loadAdjFile(initReverseAdjFilePath);
        Map<Integer, int[]> deltaForwardAdj = loadUpdateEdgeForwardList(updateFilePath);
        Map<Integer, int[]> deltaReverseAdj = loadUpdateEdgeReverseList(updateFilePath);
       prepareDatabase(initForwardAdj, initReverseAdj, deltaForwardAdj, deltaReverseAdj);
    }

    @Test
    public void testPrepareDatabase() throws Exception {
        prepareDatabase("src/test/resources/test_forward_adj.txt",
                "src/test/resources/test_reverse_adj.txt",
                "src/test/resources/test_update_edge_list2.txt");
        int key1 = 1;
        int ans1f1[] = {2,4,5,8};
        int ans1f2[] = {2,4,5};
        int ans1r1[] = {};
        int ans1r2[] = {7};
        assertArrayEquals(client.get(1, AdjType.EITHER, AdjType.FORWARD, true, 0), ans1f1);
        assertArrayEquals(client.get(1, AdjType.EITHER, AdjType.FORWARD, false, 0), ans1f2);
        assertArrayEquals(client.get(1, AdjType.EITHER, AdjType.REVERSE, false, 0), ans1r1);
        assertArrayEquals(client.get(1, AdjType.EITHER, AdjType.REVERSE, true, 0), ans1r2);
        int ans8_1[] = {2,4};
        assertArrayEquals(client.get(8, AdjType.UNALTERED, AdjType.FORWARD, true, 0), ans8_1);
        assertArrayEquals(client.get(8, AdjType.UNALTERED, AdjType.FORWARD, false, 0), ans8_1);
        int ans8_2[] = {};
        assertArrayEquals(client.get(8, AdjType.UNALTERED, AdjType.REVERSE, true, 0), ans8_2);
        assertArrayEquals(client.get(8, AdjType.UNALTERED, AdjType.REVERSE, false, 0), ans8_2);
        int ans8_3[] = {2,3,4};
        assertArrayEquals(client.get(8, AdjType.EITHER, AdjType.FORWARD, true, 0), ans8_3);
        int ans8_4[] = {1};
        assertArrayEquals(client.get(8, AdjType.EITHER, AdjType.REVERSE, true, 0), ans8_4);
    }

    public void testExecutionPlan(String planPath, int numVertex) throws Exception{
        File planFile = new File(planPath);
        assertTrue(planFile.exists());
        String execPlan[] = DynamicSubgraphEnumerationGeneric2.loadExecutionPlan(planFile);
        EnumeratorWithNoDB enumerator = new EnumeratorWithNoDB(execPlan.length);
        enumerator.construct(execPlan);
        IncrementalExecutionPlan iPlan = new IncrementalExecutionPlan(execPlan);
        Properties properties = new Properties();
        properties.setProperty(MyConf.TIMESTAMP, "0");
        properties.setProperty(MyConf.ENABLE_LOAD_BALANCE, "false");
        InMemoryCollector collector = new InMemoryCollector();
        InMemoryCollector collector2 = new InMemoryCollector();
        long totalCount = 0, totalCount2 = 0;
        for (int i = 0; i <= numVertex; i++) {
            int delta[] = client.get(i, AdjType.DELTA, AdjType.FORWARD, true, 0);
            if (delta.length == 0) continue;
            VertexCentricAnalysisTask task = new VertexCentricAnalysisTask(i, delta, 0, delta.length - 1);
            enumerator.prepare(client, properties);
            enumerator.executeVertexTask(i, task, collector);
            LocalSearchTask task2 = new LocalSearchTask(i, delta, 0);
            LocalSearchSubTask subtask2 = new LocalSearchSubTask(iPlan, properties);
            subtask2.resetTask(i, task2,0);
            subtask2.execute(client, collector2, 4,1);
            if (collector.getDeltaCount() != collector2.getDeltaCount()) {
                Tuple2<List<int[]>, List<int[]>> diff = collector.compare(collector2);
                List<int[]> missingResults = diff._1();
                List<int[]> falseResults = diff._2();
                System.err.println(missingResults);
                System.err.println(falseResults);
            }
            assertEquals(collector.getDeltaCount(), collector2.getDeltaCount());
            totalCount += collector.getDeltaCount();
            totalCount2 += collector2.getDeltaCount();
            collector.cleanup();
            collector2.cleanup();
        }
        System.out.println("Test Execution Plan Finish. |V|=" + numVertex
                + ", R1=" + totalCount
                + ", R2=" + totalCount2);
    }

    @Test
    public void testEnumeration() throws Exception {
        String planDir = "src/test/resources/diamond";
        prepareDatabase("src/test/resources/test_forward_adj.txt",
                "src/test/resources/test_reverse_adj.txt",
                "src/test/resources/test_update_edge_list2.txt");
        File planDirFile = new File(planDir);
        for (File subfile: planDirFile.listFiles()) {
            testExecutionPlan(subfile.getAbsolutePath(), 8);
        }
    }

    class InMemoryCollector implements MatchesCollectorInterface {

        public class MatchResult implements Comparable {
            public int[] getMatch() {
                return match;
            }

            public void setMatch(int[] match) {
                this.match = match;
            }

            public MatchResult(int[] match) {
                this.match = match.clone();
            }

            private int match[];

            @Override
            public int compareTo(Object o) {
                if (o instanceof MatchResult) {
                    int arr1[] = match;
                    int arr2[] = ((MatchResult)o).match;
                    if (arr1.length != arr2.length) return arr1.length - arr2.length;
                    for (int i = 0; i < arr1.length; i++) {
                        if (arr1[i] != arr2[i]) return arr1[i] - arr2[i];
                    }
                    return 0;
                } else return o.hashCode() - this.hashCode();
            }
        }

        TreeSet<MatchResult> newMatches = new TreeSet<>();
        TreeSet<MatchResult> deletedMathces = new TreeSet<>();

        @Override
        public void offer(int[] match, int flag) {
            synchronized(this) {
                if (flag == 1)
                    newMatches.add(new MatchResult(match));
                if (flag == -1)
                    deletedMathces.add(new MatchResult(match));
            }
        }

        @Override
        public long getDeltaCount() {
            return newMatches.size() + deletedMathces.size();
        }

        @Override
        public long getCounteractCount() {
            return 0;
        }

        @Override
        public void cleanup() {
            newMatches.clear();
            deletedMathces.clear();
        }

        private String arrayToStr(int arr[]) {
            return Arrays.toString(arr);
        }
        public void printMatches() {
            System.out.println(" --> new matches:");
            for (MatchResult m : newMatches) {
                int arr[] = m.match;
                System.out.println(Arrays.toString(arr));
            }
            System.out.println(" --> deleted matches:");
            for (MatchResult m : deletedMathces) {
                System.out.println(Arrays.toString(m.match));
            }
        }

        public Tuple2<List<int[]>, List<int[]>> compare(InMemoryCollector otherCollector) {
            Set<MatchResult> thisMatch = com.google.common.collect.Sets.union(newMatches, deletedMathces);
            Set<MatchResult> otherMatch = com.google.common.collect.Sets.union(otherCollector.newMatches, otherCollector.deletedMathces);
            Set<MatchResult> onlyInThis = com.google.common.collect.Sets.difference(thisMatch, otherMatch);
            Set<MatchResult> onlyInOther = com.google.common.collect.Sets.difference(otherMatch, thisMatch);
            List<int[]> onlyInThisList = onlyInThis.stream().map(m -> m.match).collect(Collectors.toList());
            List<int[]> onlyInOtherList = onlyInOther.stream().map(m -> m.match).collect(Collectors.toList());
            return new Tuple2<>(onlyInThisList, onlyInOtherList);
        }
    }

    private Tuple2<Map<Integer, int[]>, Map<Integer, int[]>> generateRandomERGraph(int numVertex, int numEdge, long seed) {
        Random random = new Random(seed);
        Int2ObjectOpenHashMap<IntSet> forwardAdjMap = new Int2ObjectOpenHashMap<>();
        Int2ObjectOpenHashMap<IntSet> reverseAdjMap = new Int2ObjectOpenHashMap<>();
        for (int i = 0; i < numVertex + 1; i++) {
            forwardAdjMap.put(i, new IntArraySet());
            reverseAdjMap.put(i, new IntArraySet());
        }
        for (int i = 0; i < numEdge; i++) {
            int src = Math.abs(random.nextInt(numVertex));
            int dst = Math.abs(random.nextInt(numVertex));
            while (dst == src || forwardAdjMap.get(src).contains(dst))
                dst = random.nextInt(numVertex);
            forwardAdjMap.get(src).add(dst);
            reverseAdjMap.get(dst).add(src);
        }
        Map<Integer, int[]> forwardAdj = new HashMap<>();
        for (int vid : forwardAdjMap.keySet()) {
            int adj[] = forwardAdjMap.get(vid).toIntArray();
            Arrays.sort(adj);
            forwardAdj.put(vid, adj);
        }
        Map<Integer, int[]> reverseAdj = new HashMap<>();
        for (int vid : reverseAdjMap.keySet()) {
            int adj[] = reverseAdjMap.get(vid).toIntArray();
            Arrays.sort(adj);
            reverseAdj.put(vid, adj);
        }
        return new Tuple2<>(forwardAdj, reverseAdj);
    }

    @Test
    public void testGenerateDeltaAdjMap() {
        Map<Integer, int[]> oldMap = new HashMap<>();
        Map<Integer, int[]> newMap = new HashMap<>();
        int old1[] = {1,2,3};
        int new1[] = {2,3,4};
        int d1[] = {CommonFunctions.setSignBitTo1(1), CommonFunctions.setSignBitTo0(4)};
        int old2[] = {1,2,3};
        int new2[] = {1,2,3};
        int d2[] = {};
        int old3[] = {4,5,6};
        int new3[] = {4, 6};
        int d3[] = {CommonFunctions.setSignBitTo1(5)};
        int old4[] = {7, 8, 9};
        int new4[] = {7, 8, 9, 10};
        int d4[] = {CommonFunctions.setSignBitTo0(10)};
        oldMap.put(1, old1);
        oldMap.put(2, old2);
        oldMap.put(3, old3);
        oldMap.put(4, old4);
        newMap.put(1, new1);
        newMap.put(2, new2);
        newMap.put(3, new3);
        newMap.put(4, new4);
        Map<Integer, int[]> deltaMap = generateDeltaAdjMap(oldMap, newMap);
        assertArrayEquals(deltaMap.get(1), d1);
        assertArrayEquals(deltaMap.get(2), d2);
        assertArrayEquals(deltaMap.get(3), d3);
        assertArrayEquals(deltaMap.get(4), d4);
    }

    private Map<Integer, int[]> generateDeltaAdjMap(Map<Integer, int[]> oldAdjMap, Map<Integer, int[]> newAdjMap) {
        Map<Integer, int[]> delta = new HashMap<>();
        for (int vid : oldAdjMap.keySet()) {
            IntArraySet oldAdj = new IntArraySet(oldAdjMap.get(vid));
            IntArraySet newAdj = new IntArraySet(newAdjMap.get(vid));
            IntArraySet deltaDelete = oldAdj.clone();
            IntArraySet deltaNew = newAdj.clone();
            deltaDelete.removeAll(newAdj);
            deltaNew.removeAll(oldAdj);
            IntArrayList deltaAdj = new IntArrayList();
            deltaDelete.stream().forEach(v -> deltaAdj.add(CommonFunctions.setSignBitTo1(v)));
            deltaNew.stream().forEach(v -> deltaAdj.add(CommonFunctions.setSignBitTo0(v)));
            Collections.sort(deltaAdj, (a,b) -> CommonFunctions.setSignBitTo0(a) - CommonFunctions.setSignBitTo0(b));
            delta.put(vid, deltaAdj.toIntArray());
        }
        return delta;
    }

    @Test
    public void testExecutionPlanWithRandomERGraphRunner() throws Exception {
        String planDir = "src/test/resources/";
        testExecutionPlanExecutionWithRandomERGraph(planDir + "triangle");
        testExecutionPlanExecutionWithRandomERGraph(planDir + "diamond");
        testExecutionPlanExecutionWithRandomERGraph(planDir + "house");
    }


    public void testExecutionPlanExecutionWithRandomERGraph(String planDir) throws Exception {
        int numVertex = 91;
        int numEdges = 931;
        Tuple2<Map<Integer, int[]>, Map<Integer, int[]>> g1 = generateRandomERGraph(numVertex, numEdges, System.currentTimeMillis());
        Tuple2<Map<Integer, int[]>, Map<Integer, int[]>> g2 = generateRandomERGraph(numVertex, numEdges, System.currentTimeMillis());
        Map<Integer, int[]> deltaForwardMap = generateDeltaAdjMap(g1._1, g2._1);
        Map<Integer, int[]> deltaReverseMap = generateDeltaAdjMap(g1._2, g2._2);
        prepareDatabase(g1._1, g1._2, deltaForwardMap, deltaReverseMap);
        File planDirFile = new File(planDir);
        for (File subfile: planDirFile.listFiles()) {
            testExecutionPlan(subfile.getAbsolutePath(), numVertex);
        }
    }

    public void runExecutionPlanWithTaskSplit(String planPath, int numVertex,
                                              String intraNodeBalanceThreshold, String interNodeBalanceThreshold) throws Exception {
        File planFile = new File(planPath);
        assertTrue(planFile.exists());
        String execPlan[] = DynamicSubgraphEnumerationGeneric2.loadExecutionPlan(planFile);
        EnumeratorWithNoDB enumerator = new EnumeratorWithNoDB(execPlan.length);
        enumerator.construct(execPlan);
        IncrementalExecutionPlan iPlan = new IncrementalExecutionPlan(execPlan);
        Properties properties = new Properties();
        properties.setProperty(MyConf.TIMESTAMP, "0");
        properties.setProperty(MyConf.ENABLE_LOAD_BALANCE, "true");
        int depth = iPlan.patternVertexNum;
        properties.setProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTRANODE, intraNodeBalanceThreshold);
        properties.setProperty(MyConf.LOAD_BALANCE_THRESHOLD_INTERNODE, interNodeBalanceThreshold);
        properties.setProperty(MyConf.NUM_EXECUTORS, "2");
        properties.setProperty(MyConf.NUM_WORKING_THREADS, "4");
        ForkJoinPool pool = new ForkJoinPool(2);

        InMemoryCollector collector = new InMemoryCollector();
        InMemoryCollector collector2 = new InMemoryCollector();
        long totalCount = 0, totalCount2 = 0;
        int taskCount = 0;
        int subTaskCount = 0;
        for (int i = 0; i <= numVertex; i++) {
            int delta[] = client.get(i, AdjType.DELTA, AdjType.FORWARD, true, 0);
            if (delta.length == 0) continue;
            //////// Correct Answer //////////////
            VertexCentricAnalysisTask task = new VertexCentricAnalysisTask(i, delta, 0, delta.length - 1);
            taskCount++;
            enumerator.prepare(client, properties);
            enumerator.executeVertexTask(i, task, collector);
            /////// New Version /////////////
            LocalSearchTask task2 = new LocalSearchTask(i, delta, 0);
            LocalSearchSubTask subtask2 = new LocalSearchSubTask(iPlan, properties);
            subtask2.resetTask(i, task2,0);
            TaskExecutionReport reportOfRound1 = subtask2.execute(client, collector2, 4, 1);
            subTaskCount++;
            Queue<Future<TaskExecutionReport>> futures = new ArrayDeque<>();
            for (LocalSearchSubTask newTask: reportOfRound1.getGeneratedSubTasks()) {
                subTaskCount++;
                Future<TaskExecutionReport> future = pool.submit(() -> {
                    return newTask.execute(client, collector2, 4, 2);
                });
                futures.offer(future);
            }
            while (!futures.isEmpty()) {
                TaskExecutionReport report = futures.remove().get();
                for (LocalSearchSubTask newTask : report.getGeneratedSubTasks()) {
                    subTaskCount++;
                    Future<TaskExecutionReport> future = pool.submit(() -> {
                        return newTask.execute(client, collector2, 4, 2);
                    });
                    futures.offer(future);
                }
            }
            if (collector.getDeltaCount() != collector2.getDeltaCount()) {
                /*
                Tuple2<List<int[]>, List<int[]>> diff = collector.compare(collector2);
                List<int[]> missingMatches = diff._1;
                List<int[]> falseMatches = diff._2();
                System.out.println(missingMatches);
                System.out.println(falseMatches);
                 */
                assertEquals(collector.getDeltaCount(), collector2.getDeltaCount());
            }
            assertEquals(collector.getDeltaCount(), collector2.getDeltaCount());
            totalCount += collector.getDeltaCount();
            totalCount2 += collector2.getDeltaCount();
            collector.cleanup();
            collector2.cleanup();
        }
        System.out.println("Test Execution Plan Finish. |V|=" + numVertex
                + ", R1=" + totalCount
                + ", R2=" + totalCount2
                + ", #tasks=" + taskCount
                + ", #subtasks=" + subTaskCount);
    }

    @Test
    public void testTaskSplit() throws Exception{
        String planDir = "src/test/resources/diamond";
        prepareDatabase("src/test/resources/test_forward_adj.txt",
                "src/test/resources/test_reverse_adj.txt",
                "src/test/resources/test_update_edge_list2.txt");
        File planDirFile = new File(planDir);
        for (File subfile: planDirFile.listFiles()) {
            runExecutionPlanWithTaskSplit(subfile.getAbsolutePath(), 8, "3,5,8", "3,15,30");
        }
    }

    public void testTaskSplitExecutionWithRandomERGraph(String planDir, long seed,
                                                        int numVertex, int numEdges,
                                                        String intraNodeThreshold, String interNodeThreshold) throws Exception {
        Tuple2<Map<Integer, int[]>, Map<Integer, int[]>> g1 = generateRandomERGraph(numVertex, numEdges, seed);
        Tuple2<Map<Integer, int[]>, Map<Integer, int[]>> g2 = generateRandomERGraph(numVertex, numEdges, seed + 127);
        Map<Integer, int[]> deltaForwardMap = generateDeltaAdjMap(g1._1, g2._1);
        Map<Integer, int[]> deltaReverseMap = generateDeltaAdjMap(g1._2, g2._2);
        prepareDatabase(g1._1, g1._2, deltaForwardMap, deltaReverseMap);
        File planDirFile = new File(planDir);
        for (File subfile: planDirFile.listFiles()) {
            runExecutionPlanWithTaskSplit(subfile.getAbsolutePath(), numVertex, intraNodeThreshold, interNodeThreshold);
        }

        //runExecutionPlanWithSplit("src/test/resources/triangle/3.txt", numVertex, threshold);
    }

    @Test
    public void runTestTaskSplitExecutionWithRandomERGraph() throws Exception {
        String planDir = "src/test/resources/";
        for (int i = 0; i < 5; i++) {
            long seed = System.currentTimeMillis();
            seed = 42;
            testTaskSplitExecutionWithRandomERGraph(planDir + "triangle", seed, 51, 472,
                    "3,8,10", "6,10,20");
            testTaskSplitExecutionWithRandomERGraph(planDir + "diamond", seed, 51, 472,
                    "3,8,10", "6,10,20");
            testTaskSplitExecutionWithRandomERGraph(planDir + "house", seed, 51, 472,
                    "3,8,10", "6,10,20");
        }

    }

}
