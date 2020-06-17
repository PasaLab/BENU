package cn.edu.nju.pasa.graph.analysis.subgraph.enumerator;

import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.storage.AbstractSimpleGraphStorage;
import cn.edu.nju.pasa.graph.storage.CacheBasedDistributedGraphStorage;
import cn.edu.nju.pasa.graph.util.HostInfoUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Properties;

/**
 * Pattern Graph: TriangleCore
 * <p>
 * Execution Plan
 * <p>
 * Final Optimal Execution Plan
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=4
 * C3:=Intersect(A1) | >f1
 * f3:=Foreach(C3)
 * A3:=GetAdj(f3) | SIZE>=4
 * T7:=TCache(f1,f3,A1,A3) | SIZE>=2
 * C5:=Intersect(T7) | >f3
 * f5:=Foreach(C5)
 * C2:=Intersect(T7) | ≠f5
 * A5:=GetAdj(f5) | SIZE>=4
 * T4:=Intersect(A3,A5) | SIZE>=2
 * T6:=TCache(f1,f5,A1,A5) | SIZE>=2
 * f2:=Foreach(C2)
 * C4:=Intersect(T4) | ≠f1,≠f2
 * f4:=Foreach(C4)
 * C6:=Intersect(T6) | ≠f2,≠f3,≠f4
 * f6:=Foreach(C6)
 * f:=ReportMatch(f1,f2,f3,f4,f5,f6)
 * Final Optimal Execution Plan - Compression
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=4
 * C3:=Intersect(A1) | >f1
 * f3:=Foreach(C3)
 * A3:=GetAdj(f3) | SIZE>=4
 * T7:=TCache(f1,f3,A1,A3) | SIZE>=2
 * C5:=Intersect(T7) | >f3
 * f5:=Foreach(C5)
 * C2:=Intersect(T7) | ≠f5
 * A5:=GetAdj(f5) | SIZE>=4
 * T4:=Intersect(A3,A5) | SIZE>=2
 * T6:=TCache(f1,f5,A1,A5) | SIZE>=2
 * C4:=Intersect(T4) | ≠f1
 * C6:=Intersect(T6) | ≠f3
 * f:=ReportMatch(f1,f3,f5,C2,C4,C6)
 * <p>
 * Created by wzk on 9/21/2018.
 */

public class TriangleCoreEnumerator implements EnumerateOnCenterVertexTaskInterface {
    AbstractSimpleGraphStorage graphStorage;
    private boolean enumerate;
    private boolean enableLoadBanlance;

    @Override
    public void prepare(Properties conf) {
        try {
            graphStorage = CacheBasedDistributedGraphStorage.getProcessLevelStorage();
            enumerate = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_ENUMERATE, "true"));
            enableLoadBanlance = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_LOAD_BALANCE, "false"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public VertexTaskStats enumerate(int startVid, VertexCentricAnalysisTask task) {
        long localCount = 0L;
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, 0L, 0L, HostInfoUtil.getHostName());
        Int2ObjectOpenHashMap<IntArrayList> localTriangleCache = new Int2ObjectOpenHashMap<>();

        long t0 = System.nanoTime();
        try {
            //f1=Init(start)
            int f1 = startVid;
            // A1=GetAdj(f1) | SIZE>=4
            int a1[] = graphStorage.get(f1);
            if (a1.length < 4) return stats;
            // C3=Intersect(A1) | >f1
            IntArrayList c3 = new IntArrayList();
            if (enableLoadBanlance) {
                int a1Balanced[] = Arrays.copyOfRange(task.getAdj(), task.getStart(), task.getEnd() + 1);
                Arrays.sort(a1Balanced);
                CommonFunctions.filterAdjListAdaptive(c3, a1Balanced, f1);
            } else {
                CommonFunctions.filterAdjListAdaptive(c3, a1, f1);
            }
            // f3=Foreach(C3)
            for (int f3 : c3) {
                // A3=GetAdj(f3) | SIZE>=4
                int a3[] = graphStorage.get(f3);
                if (a3.length < 4) continue;
                // T7=TCache(f1,f3,A1,A3) | SIZE>=2
                IntArrayList t7;
                t7 = localTriangleCache.get(f3);
                if (t7 == null) {
                    IntArrayList tri = new IntArrayList();
                    CommonFunctions.intersectTwoSortedArrayAdaptive(tri, a1, a3);
                    localTriangleCache.put(f3, tri);
                    t7 = tri;
                }
                if (t7.size() < 2) continue;
                // C5=Intersect(T7) | >f1,>f3
                IntArrayList c5 = new IntArrayList();
                for (int t7i : t7) if (t7i > f1 && t7i > f3) c5.add(t7i);
                if (c5.size() < 1) continue;
                // f5=Foreach(C5)
                for (int f5 : c5) {
                    // C2=Intersect(T7) | ≠f5
                    IntArrayList c2 = new IntArrayList();
                    for (int t7i : t7) if (t7i != f5) c2.add(t7i);
                    if (c2.size() < 1) continue;
                    // A5=GetAdj(f5)
                    int a5[] = graphStorage.get(f5);
                    if (a5.length < 4) continue;
                    // T4=Intersect(A3,A5) | SIZE>=2
                    IntArrayList t4 = new IntArrayList();
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t4, a3, a5);
                    if (t4.size() < 2) continue;
                    // T6=TCache(f1,f5,A1,A5) | SIZE>=2
                    IntArrayList t6;
                    t6 = localTriangleCache.get(f5);
                    if (t6 == null) {
                        IntArrayList tri = new IntArrayList();
                        CommonFunctions.intersectTwoSortedArrayAdaptive(tri, a1, a5);
                        localTriangleCache.put(f5, tri);
                        t6 = tri;
                    }
                    if (t6.size() < 2) continue;
                    if (enumerate) {
                        for (int f2 : c2) {
                            // C4=Intersect(T4) | ≠f1,≠f2
                            IntArrayList c4 = new IntArrayList();
                            for (int x : t4) if (x != f1 && x != f2) c4.add(x);
                            for (int f4 : c4) {
                                // C6=Intersect(T6) | ≠f2,≠f3,≠f4
                                IntArrayList c6 = new IntArrayList();
                                for (int x : t6) if (x != f2 && x != f3 && x != f4) c6.add(x);
                                for (int f6 : c6)
                                    localCount++;
                            }
                        }
                    } else {
                        // C4=Intersect(T4) | ≠f1
                        IntArrayList c4 = new IntArrayList();
                        for (int x : t4) if (x != f1) c4.add(x);
                        if (c4.size() < 1) continue;
                        // C6=Intersect(T6) | ≠f3
                        IntArrayList c6 = new IntArrayList();
                        for (int x : t6) if (x != f3) c6.add(x);
                        if (c6.size() < 1) continue;
                        localCount++;
                    } // end of enumerate
                } // end of f5
            } // end of f3
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(101);
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