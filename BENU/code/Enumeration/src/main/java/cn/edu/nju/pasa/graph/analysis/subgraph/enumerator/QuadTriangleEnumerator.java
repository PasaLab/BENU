package cn.edu.nju.pasa.graph.analysis.subgraph.enumerator;

import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.storage.CacheBasedDistributedGraphStorage;
import cn.edu.nju.pasa.graph.util.HostInfoUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Properties;

/**
 * Pattern Graph
 *
 *    u3 - u2
 *  /   \   |
 * u4 ---- u1
 *  \   /  |
 *  u5 - u6
 *
 * u3 < u5
 * u1 is the start vertex
 *
 * Execution Plan
 *
 inal Optimal Execution Plan
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=5
 C3:=Intersect(A1)
 f3:=Foreach(C3)
 C5:=Intersect(A1) | >f3
 A3:=GetAdj(f3) | SIZE>=3
 T7:=TCache(f1,f3,A1,A3) | SIZE>=2
 f5:=Foreach(C5)
 C2:=Intersect(T7) | ≠f5
 A5:=GetAdj(f5) | SIZE>=3
 T6:=TCache(f1,f5,A1,A5) | SIZE>=2
 T4:=Intersect(T7,A5)
 f2:=Foreach(C2)
 C6:=Intersect(T6) | ≠f2,≠f3
 f6:=Foreach(C6)
 C4:=Intersect(T4) | ≠f2,≠f6
 f4:=Foreach(C4)
 f:=ReportMatch(f1,f2,f3,f4,f5,f6)
 Final Optimal Execution Plan - Compression
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=5
 C3:=Intersect(A1)
 f3:=Foreach(C3)
 C5:=Intersect(A1) | >f3
 A3:=GetAdj(f3) | SIZE>=3
 T7:=TCache(f1,f3,A1,A3) | SIZE>=2
 f5:=Foreach(C5)
 C2:=Intersect(T7) | ≠f5
 A5:=GetAdj(f5) | SIZE>=3
 T6:=TCache(f1,f5,A1,A5) | SIZE>=2
 T4:=Intersect(T7,A5)
 C6:=Intersect(T6) | ≠f3
 C4:=Intersect(T4)
 f:=ReportMatch(f1,f3,f5,C2,C6,C4)
 * Created by huweiwei on 4/17/2018.
 */

public class QuadTriangleEnumerator implements EnumerateOnCenterVertexTaskInterface {
    CacheBasedDistributedGraphStorage graphStorage;
    private boolean enableLoadBanlance;
    private boolean enableEnumerate;
    @Override
    public void prepare(Properties conf) {
        try {
            graphStorage = CacheBasedDistributedGraphStorage.getProcessLevelStorage();
            enableLoadBanlance = Boolean.parseBoolean(conf.getProperty("enable.load.balance", "false"));
            enableEnumerate = Boolean.parseBoolean(conf.getProperty("enable.enumerate", "true"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public VertexTaskStats enumerate(int startVid, VertexCentricAnalysisTask task) {
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, 0, 0,
                HostInfoUtil.getHostName());
        Int2ObjectOpenHashMap<IntArrayList> localTriangleCache = new Int2ObjectOpenHashMap<>();
        IntArrayList c5 = new IntArrayList();
        IntArrayList c2 = new IntArrayList();
        IntArrayList c6 = new IntArrayList();
        IntArrayList c4 = new IntArrayList();

        IntArrayList t7;
        IntArrayList t4 = new IntArrayList();

        long localCount = 0L;
        long t0 = System.nanoTime();
        try {
            // f1=Init(start)
            int f1 = startVid;
            // A1=GetAdj(f1) | SIZE>=5
            int a1[] = graphStorage.get(f1);
            if (a1.length < 5) return stats;
            // C3=Intersect(A1)
            IntArrayList c3;
            if (enableLoadBanlance) {
                int start = task.getStart();
                int end = task.getEnd();
                int taskA1[] = Arrays.copyOfRange(task.getAdj(), start, end + 1);
                Arrays.sort(taskA1);
                c3 = new IntArrayList(taskA1);
            } else {
                c3 = new IntArrayList(a1);
            }
            // f3=Foreach(C3)
            for (int f3: c3) {
                // C5=Intersect(A1) | >f3
                CommonFunctions.filterAdjListAdaptive(c5, a1, f3);
                if (c5.size() < 1) break; // because the following f3 are bigger than the current f3
                // A3=GetAdj(f3) | SIZE>=3
                int a3[] = graphStorage.get(f3);
                if (a3.length < 3) continue;
                // T7=TCache(f1,f3,A1,A3) | SIZE>=2
                t7 = localTriangleCache.get(f3);
                if (t7 == null) {
                    IntArrayList triangle = new IntArrayList();
                    CommonFunctions.intersectTwoSortedArrayAdaptive(triangle, a1, a3);
                    localTriangleCache.put(f3, triangle);
                    t7 = triangle;
                }
                if (t7.size() < 2) continue;
                // f5=Foreach(C5)
                for (int f5: c5) {
                    // C2=Intersect(T7) | ≠f5
                    c2.clear();
                    for (int x: t7) if (x != f5) c2.add(x);
                    if (c2.size() < 1) continue;
                    // A5=GetAdj(f5) | SIZE>=3
                    int a5[] = graphStorage.get(f5);
                    if (a5.length < 3) continue;
                    // T6=TCache(f1,f5,A1,A5) | SIZE>=2
                    IntArrayList t6;
                    t6 = localTriangleCache.get(f5);
                    if (t6 == null) {
                        IntArrayList triangle = new IntArrayList();
                        CommonFunctions.intersectTwoSortedArrayAdaptive(triangle, a1, a5);
                        localTriangleCache.put(f5, triangle);
                        t6 = triangle;
                    }
                    if (t6.size() < 2) continue;
                    // T4=Intersect(T7,A5)
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t4, t7, a5);
                    if (t4.size() < 1) continue;
                    if (enableEnumerate) {
                        // f2=Foreach(C2)
                        for (int f2: c2) {
                            // C6=Intersect(T6) | ≠f2,≠f3
                            c6.clear();
                            for (int x: t6) if (x != f2 && x != f3) c6.add(x);
                            // f6=Foreach(C6)
                            for (int f6: c6) {
                                // C4=Intersect(T4) | ≠f2,≠f6
                                c4.clear();
                                for (int x:t4) if (x != f2 && x != f6) c4.add(x);
                                // f4=Foreach(C4)
                                for (int f4: c4) {
                                    // F=ReportEmbedding(f1,f2,f3,f4,f5,f6)
                                    localCount++;
                                } // f4
                            } // f6
                        } // f2
                    } else {
                        //C6=Intersect(T6) | ≠f3
                        c6.clear();
                        for(int x: t6) if (x != f3) c6.add(x);
                        if (c6.size() < 1) continue;
                        //C4=Intersect(T4)
                        c4 = t4;
                        localCount++;
                    }
                } // f5
            } // f3
        }catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        long t1 = System.nanoTime();
        stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, localCount, (t1 - t0),
                HostInfoUtil.getHostName());

        return stats;
    }

    @Override
    public void cleanup() {

    }
}
