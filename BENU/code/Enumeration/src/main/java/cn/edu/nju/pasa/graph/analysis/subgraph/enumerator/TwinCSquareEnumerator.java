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
 * Pattern Graph: TwinCSquare
 *
 * Execution Plan
 *
 Final Optimal Execution Plan
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=4
 C4:=Intersect(A1) | >f1
 f4:=Foreach(C4)
 A4:=GetAdj(f4) | SIZE>=4
 T7:=TCache(f1,f4,A1,A4) | SIZE>=2
 C2:=Intersect(T7)
 f2:=Foreach(C2)
 C5:=Intersect(T7) | ≠f2
 A2:=GetAdj(f2) | SIZE>=3
 T3:=Intersect(A2,A4) | SIZE>=2
 f5:=Foreach(C5)
 C3:=Intersect(T3) | ≠f1,≠f5
 A5:=GetAdj(f5) | SIZE>=3
 T6:=TCache(f1,f5,A1,A5) | SIZE>=2
 f3:=Foreach(C3)
 C6:=Intersect(T6) | ≠f2,≠f3,≠f4
 f6:=Foreach(C6)
 f:=ReportMatch(f1,f2,f3,f4,f5,f6)
 Final Optimal Execution Plan - Compression
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=4
 C4:=Intersect(A1) | >f1
 f4:=Foreach(C4)
 A4:=GetAdj(f4) | SIZE>=4
 T7:=TCache(f1,f4,A1,A4) | SIZE>=2
 C2:=Intersect(T7)
 f2:=Foreach(C2)
 C5:=Intersect(T7) | ≠f2
 A2:=GetAdj(f2) | SIZE>=3
 T3:=Intersect(A2,A4) | SIZE>=2
 f5:=Foreach(C5)
 C3:=Intersect(T3) | ≠f1,≠f5
 A5:=GetAdj(f5) | SIZE>=3
 T6:=TCache(f1,f5,A1,A5) | SIZE>=2
 C6:=Intersect(T6) | ≠f2,≠f4
 f:=ReportMatch(f1,f4,f2,f5,C3,C6)
 *
 * Created by wzk on 9/20/2018.
 */

public class TwinCSquareEnumerator implements EnumerateOnCenterVertexTaskInterface {
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

        Int2ObjectOpenHashMap<IntArrayList> localTriangleCache = new Int2ObjectOpenHashMap<>();

        long localCount= 0L;
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, 0L, 0L, HostInfoUtil.getHostName());

        IntArrayList t7 = new IntArrayList();
        IntArrayList t3 = new IntArrayList();
        IntArrayList t6 = new IntArrayList();
        IntArrayList c2 = new IntArrayList();
        IntArrayList c3 = new IntArrayList();
        IntArrayList c5 = new IntArrayList();
        //IntArrayList c6 = new IntArrayList();

        long t0 = System.nanoTime();
        try {
            int f1 = startVid;
            int a1[];
            a1 = graphStorage.get(f1);
            if (a1.length < 4) return stats;
            IntArrayList c4 = new IntArrayList();
            if(enableLoadBanlance) {
                int loadBalanceStart = task.getStart();
                int loadBalanceEnd = task.getEnd();
                int a1Balanced[] = Arrays.copyOfRange(task.getAdj(), loadBalanceStart, loadBalanceEnd+1);
                Arrays.sort(a1Balanced);
                CommonFunctions.filterAdjListAdaptive(c4, a1Balanced, f1);
            } else {
                CommonFunctions.filterAdjListAdaptive(c4, a1, f1);
            }
            for (int f4: c4) {
                int a4[] = graphStorage.get(f4);
                if (a4.length < 4) continue;
                t7 = localTriangleCache.get(f4);
                if (t7 == null) {
                    t7 = new IntArrayList();
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t7, a1, a4);
                    localTriangleCache.put(f4, t7);
                }
                if (t7.size() < 2) continue;
                c2 = t7;
                for(int f2: c2) {
                    c5.clear();
                    for(int x:t7) if (x != f2) c5.add(x);
                    if (c5.size() < 1) continue;
                    int a2[] = graphStorage.get(f2);
                    if (a2.length < 3) continue;
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t3, a2, a4);
                    if (t3.size() < 2) continue;
                    for(int f5: c5) {
                        c3.clear();
                        for(int x: t3) if (x != f1 && x != f5) c3.add(x);
                        if (c3.size() < 1) continue;
                        int a5[] = graphStorage.get(f5);
                        if (a5.length < 3) continue;
                        t6 = localTriangleCache.get(f5);
                        if (t6 == null) {
                            t6 = new IntArrayList();
                            CommonFunctions.intersectTwoSortedArrayAdaptive(t6, a1, a5);
                            localTriangleCache.put(f5, t6);
                        }
                        if (t6.size() < 2) continue;
                        if (enumerate) {
                            // TODO: Decompresse
                            // f3=Foreach(C3)
                            IntArrayList c6 = new IntArrayList();
                            for (int f3: c3) {
                                c6.clear();
                                // C6=Intersect(T6) | ≠f2,≠f3,≠f4
                                for (int x: t6) if (x != f2 && x != f3 && x != f4) c6.add(x);
                                // f6=Foreach(C6)
                                for (int f6: c6)
                                    localCount++;
                            }
                        } else {
                            /*
                            c6.clear();
                            for(int x:t6) if (x != f2 && x != f4) c6.add(x);
                            if (c6.size() < 1) continue;
                            */
                            int c6[] = t6.toIntArray();
                            int pos = Arrays.binarySearch(c6, f2);
                            if (pos >= 0) c6[pos] = -1; // remove f2
                            pos = Arrays.binarySearch(c6, f4);
                            if (pos >= 0) c6[pos] = -1; // remove f4
                            localCount++;
                        }
                    }
                }
            }
        }catch (Exception e) {
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