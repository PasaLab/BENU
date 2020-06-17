package cn.edu.nju.pasa.graph.analysis.subgraph.enumerator;

import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.storage.AbstractSimpleGraphStorage;
import cn.edu.nju.pasa.graph.storage.CacheBasedDistributedGraphStorage;
import cn.edu.nju.pasa.graph.util.HostInfoUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Properties;

/**
 * Pattern Graph
 *
 * u1  -  u4
 * |  X(u5)|
 * u2 - u3
 *
 * u1 < u2, u1 < u3, u1 < u4, u2 < u4
 * u5 is the start vertex
 *
 Final Optimal Execution Plan
 ---------------------------------------------------
 f5:=Init(start)
 A5:=GetAdj(f5) | SIZE>=4
 C1:=Intersect(A5)
 f1:=Foreach(C1)
 C3:=Intersect(A5) | >f1
 A1:=GetAdj(f1) | SIZE>=3
 T7:=Intersect(A5,A1) | SIZE>=2
 f3:=Foreach(C3)
 A3:=GetAdj(f3) | SIZE>=3
 T6:=Intersect(T7,A3) | SIZE>=2
 C2:=Intersect(T6) | >f1
 f2:=Foreach(C2)
 C4:=Intersect(T6) | >f2
 f4:=Foreach(C4)
 f:=ReportMatch(f1,f2,f3,f4,f5)
 Final Optimal Execution Plan - Compression
 ---------------------------------------------------
 f5:=Init(start)
 A5:=GetAdj(f5) | SIZE>=4
 C1:=Intersect(A5)
 f1:=Foreach(C1)
 C3:=Intersect(A5) | >f1
 A1:=GetAdj(f1) | SIZE>=3
 T7:=Intersect(A5,A1) | SIZE>=2
 f3:=Foreach(C3)
 A3:=GetAdj(f3) | SIZE>=3
 T6:=Intersect(T7,A3) | SIZE>=2
 C2:=Intersect(T6) | >f1
 C4:=Intersect(T6) | >f1
 f:=ReportMatch(f5,f1,f3,C2,C4)
 *
 * Created by huweiwei on 4/15/2018.
 */

public class SolarSquareEnumerator implements EnumerateOnCenterVertexTaskInterface {
    AbstractSimpleGraphStorage graphStorage;
    private boolean enableLoadBanlance;
    private boolean enableEnuemrate;
    @Override
    public void prepare(Properties conf) {
        try {
            graphStorage = CacheBasedDistributedGraphStorage.getProcessLevelStorage();
            enableLoadBanlance = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_LOAD_BALANCE, "false"));
            enableEnuemrate = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_ENUMERATE, "true"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public VertexTaskStats enumerate(int startVid, VertexCentricAnalysisTask task) {
        IntArrayList t6 = new IntArrayList();
        IntArrayList t7 = new IntArrayList();
        IntArrayList c1 = new IntArrayList();
        IntArrayList c2 = new IntArrayList();
        IntArrayList c3 = new IntArrayList();
        IntArrayList c4 = new IntArrayList();
        long localCount = 0L;
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, 0, 0L,
                HostInfoUtil.getHostName());
        long t0 = System.nanoTime();
        try {
            // f5=Init(start)
            int f5 = startVid;
            // A5=GetAdj(f5) | SIZE>=4
            int a5[] = graphStorage.get(f5);
            if (a5.length < 4) return stats;
            // C1=Intersect(A5)
            if (enableLoadBanlance) {
                int start = task.getStart();
                int end = task.getEnd();
                int taskA5[] = Arrays.copyOfRange(task.getAdj(), start, end + 1);
                Arrays.sort(taskA5);
                c1 = new IntArrayList(taskA5);
            } else {
                c1 = new IntArrayList(a5);
            }
            // f1=Foreach(C1)
            for (int f1: c1) {
                // C3=Intersect(A5) | >f1
                CommonFunctions.filterAdjListAdaptive(c3, a5, f1);
                if (c3.size() < 1) break; // because the following f1 are bigger than the current f1
                // A1=GetAdj(f1) | SIZE>=3
                int[] a1 = graphStorage.get(f1);
                if (a1.length < 3) continue;
                // T7=Intersect(A5,A1) | SIZE>=2
                CommonFunctions.intersectTwoSortedArrayAdaptive(t7, a5, a1);
                if (t7.size() < 2) continue;
                // f3=Foreach(C3)
                for (int f3: c3) {
                    // A3=GetAdj(f3) | SIZE>=3
                    int a3[] = graphStorage.get(f3);
                    if (a3.length < 3) continue;
                    // T6=Intersect(T7,A3) | SIZE>=2
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t6, t7, a3);
                    if (t6.size()  < 2) continue;
                    // C2=Intersect(T6) | >f1
                    CommonFunctions.filterAdjListAdaptive(c2, t6, f1);
                    if (c2.size() < 1) continue;
                    if (enableEnuemrate) {
                        // f2=Foreach(C2)
                        for(int f2: c2) {
                            //  C4=Intersect(T6) | >f1,>f2
                            CommonFunctions.filterAdjListAdaptive(c4, c2, f2);
                            if (c4.size() < 1) continue;
                            // f4=Foreach(C4)
                            for(int f4: c4)
                                localCount++;
                        }
                    } else {
                        c4 = c2;
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
