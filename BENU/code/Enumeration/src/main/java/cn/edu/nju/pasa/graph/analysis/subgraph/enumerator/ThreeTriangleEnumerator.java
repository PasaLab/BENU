package cn.edu.nju.pasa.graph.analysis.subgraph.enumerator;

import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
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
 *         u1
 *    /  |  \  \
 * u2 - u3 - u4 - u5
 *
 * u3 < u4
 * u1 is the start vertex
 *
 Final Optimal Execution Plan
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=4
 C3:=Intersect(A1)
 f3:=Foreach(C3)
 A3:=GetAdj(f3) | SIZE>=3
 T6:=TCache(f1,f3,A1,A3) | SIZE>=2
 C4:=Intersect(T6) | >f3
 f4:=Foreach(C4)
 C2:=Intersect(T6) | ≠f4
 A4:=GetAdj(f4) | SIZE>=3
 T5:=TCache(f1,f4,A1,A4) | SIZE>=2
 f2:=Foreach(C2)
 C5:=Intersect(T5) | ≠f2,≠f3
 f5:=Foreach(C5)
 f:=ReportMatch(f1,f2,f3,f4,f5)
 Final Optimal Execution Plan - Compression
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=4
 C3:=Intersect(A1)
 f3:=Foreach(C3)
 A3:=GetAdj(f3) | SIZE>=3
 T6:=TCache(f1,f3,A1,A3) | SIZE>=2
 C4:=Intersect(T6) | >f3
 f4:=Foreach(C4)
 C2:=Intersect(T6) | ≠f4
 A4:=GetAdj(f4) | SIZE>=3
 T5:=TCache(f1,f4,A1,A4) | SIZE>=2
 C5:=Intersect(T5) | ≠f3
 f:=ReportMatch(f1,f3,f4,C2,C5)
 *
 * Created by huweiwei on 4/15/2018.
 */

public class ThreeTriangleEnumerator implements EnumerateOnCenterVertexTaskInterface {
    CacheBasedDistributedGraphStorage graphStorage;
    private boolean enableLoadBanlance;
    private boolean enableEnumerate;
    @Override
    public void prepare(Properties conf) {
        try {
            graphStorage = CacheBasedDistributedGraphStorage.getProcessLevelStorage();
            enableLoadBanlance = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_LOAD_BALANCE, "false"));
            enableEnumerate = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_ENUMERATE, "true"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public VertexTaskStats enumerate(int startVid, VertexCentricAnalysisTask task) {
        IntArrayList c2 = new IntArrayList();
        IntArrayList c4 = new IntArrayList();
        Int2ObjectOpenHashMap<IntArrayList> localTriangleCache = new Int2ObjectOpenHashMap<>();

        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, 0, 0,
                HostInfoUtil.getHostName());
        long localCount = 0L;
        long t0 = System.nanoTime();
        try {
            // f1=Init(start)
            int f1 = startVid;
            // A1=GetAdj(f1) | SIZE>=4
            int a1[] = graphStorage.get(f1);
            if (a1.length < 4) return stats;
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
            for(int f3: c3) {
                // A3=GetAdj(f3) | SIZE>=3
                int a3[] = graphStorage.get(f3);
                if (a3.length < 3) continue;
                // T6=TCache(f1,f3,A1,A3) | SIZE>=2
                IntArrayList t6 = localTriangleCache.get(f3);
                if (t6 == null) {
                    IntArrayList triangle = new IntArrayList();
                    CommonFunctions.intersectTwoSortedArrayAdaptive(triangle, a1, a3);
                    localTriangleCache.put(f3, triangle);
                    t6 = triangle;
                }
                if (t6.size() < 2) continue;
                // C4=Intersect(T6) | >f3
                CommonFunctions.filterAdjListAdaptive(c4, t6, f3);
                // f4=Foreach(C4)
                for (int f4 : c4) {
                    // C2=Intersect(T6) | ≠f4
                    c2.clear();
                    for (int x : t6) if (x != f4) c2.add(x);
                    if (c2.size() < 1) continue;
                    // A4=GetAdj(f4) | SIZE>=3
                    int a4[] = graphStorage.get(f4);
                    if (a4.length < 3) continue;
                    // T5=TCache(f1,f4,A1,A4) | SIZE>=2
                    IntArrayList t5 = localTriangleCache.get(f4);
                    if (t5 == null) {
                        IntArrayList triangle = new IntArrayList();
                        CommonFunctions.intersectTwoSortedArrayAdaptive(triangle, a1, a4);
                        localTriangleCache.put(f4, triangle);
                        t5 = triangle;
                    }
                    if (t5.size() < 2) continue;
                    if (enableEnumerate) {
                        // f2=Foreach(C2)
                        IntArrayList c5 = new IntArrayList();
                        for (int f2 : c2) {
                            // C5=Intersect(T5) | ≠f2,≠f3
                            c5.clear();
                            for (int x : t5) if (x != f2 && x != f3) c5.add(x);
                            // f5=Foreach(C5)
                            for (int f5 : c5) {
                                localCount++;
                            }
                        }
                    } else {
                        // C5=Intersect(T5) | ≠f3
                        /*
                        c5.clear();
                        for (int x : t5) if (x != f3) c5.add(x);
                        if (c5.size() < 1) continue;
                        */
                        int c5[] = t5.toIntArray();
                        int pos = Arrays.binarySearch(c5, f3);
                        if (pos >= 0) c5[pos] = -1; // remove u3 from c5 by marking it to negative value
                        localCount++;
                    }
                }
            }
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