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
 *    u5
 *  /   \
 * u1 - u4
 * |  X |
 * u2 - u3
 *
 * u1 < u4, u2 < u3
 * u1 is the start vertex
 *
 * Final Optimal Execution Plan
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=4
 * C4:=Intersect(A1) | >f1
 * f4:=Foreach(C4)
 * A4:=GetAdj(f4) | SIZE>=4
 * T6:=Intersect(A1,A4) | SIZE>=3
 * C2:=Intersect(T6)
 * f2:=Foreach(C2)
 * C5:=Intersect(T6) | ≠f2
 * A2:=GetAdj(f2) | SIZE>=3
 * T3:=Intersect(A2,T6)
 * f5:=Foreach(C5)
 * C3:=Intersect(T3) | >f2,≠f5
 * f3:=Foreach(C3)
 * f:=ReportMatch(f1,f2,f3,f4,f5)
 * Final Optimal Execution Plan - Compression
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=4
 * C4:=Intersect(A1) | >f1
 * f4:=Foreach(C4)
 * A4:=GetAdj(f4) | SIZE>=4
 * T6:=Intersect(A1,A4) | SIZE>=3
 * C2:=Intersect(T6)
 * f2:=Foreach(C2)
 * C5:=Intersect(T6) | ≠f2
 * A2:=GetAdj(f2) | SIZE>=3
 * T3:=Intersect(A2,T6)
 * C3:=Intersect(T3) | >f2
 * f:=ReportMatch(f1,f4,f2,C5,C3)
 *
 * Created by huweiwei on 4/15/2018.
 */

public class Near5CliqueEnumerator implements EnumerateOnCenterVertexTaskInterface {
    AbstractSimpleGraphStorage graphStorage;
    private boolean enableLoadBanlance;
    private boolean enumerate;
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
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, 0, 0,
                HostInfoUtil.getHostName());
        long localCount = 0L;
        long t0 = System.nanoTime();
        IntArrayList c4 = new IntArrayList(),
                t6 = new IntArrayList(),
                c2 = new IntArrayList(),
                c5 = new IntArrayList(),
                t3 = new IntArrayList(),
                c3 = new IntArrayList();

        try{
            // * f1=Init(start)
            int f1 = startVid;
            // * A1=GetAdj(f1) | SIZE>=4
            int a1[] = graphStorage.get(f1);
            if (a1.length < 4) return stats;
            // * C4=Intersect(A1) | >f1
            if (enableLoadBanlance) {
                int start = task.getStart();
                int end = task.getEnd();
                int taskA1[] = Arrays.copyOfRange(task.getAdj(), start, end + 1);
                Arrays.sort(taskA1);
                CommonFunctions.filterAdjListAdaptive(c4, taskA1, f1);
            } else {
                CommonFunctions.filterAdjListAdaptive(c4, a1, f1);
            }
            // * f4=Foreach(C4)
            for (int f4: c4) {
                // * A4=GetAdj(f4) | SIZE>=4
                int a4[] = graphStorage.get(f4);
                if (a4.length < 4) continue;
                // * T6=Intersect(A1,A4) | SIZE>=3
                CommonFunctions.intersectTwoSortedArrayAdaptive(t6, a1, a4);
                if (t6.size() < 3) continue;
                // * C2=Intersect(T6)
                c2 = t6;
                // * f2=Foreach(C2)
                for (int f2: c2) {
                    // * C5=Intersect(T6) | ≠f2
                    c5.clear();
                    for(int x: t6) if (x != f2) c5.add(x);
                    if (c5.size() < 1) continue;
                    // * A2=GetAdj(f2) | SIZE>=3
                    int a2[] = graphStorage.get(f2);
                    if (a2.length < 3) continue;
                    // * T3=Intersect(A2,T6)
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t3, t6, a2);
                    if (t3.size() < 1) continue;
                    if (enumerate) {
                        // * f5=Foreach(C5)
                        for (int f5: c5) {
                            // * C3=Intersect(T3) | >f2,≠f5
                            c3.clear();
                            for (int x: t3) if (x > f2 && x != f5) c3.add(x);
                            if (c3.size() < 1) continue;
                            // * f3=Foreach(C3)
                            for (int f3: c3) {
                                // * F=ReportEmbedding(f1,f2,f3,f4,f5)
                                localCount++;
                            }
                        }
                    } else {
                         // * C3=Intersect(T3) | >f2
                        CommonFunctions.filterAdjListAdaptive(c3, t3, f2);
                        if (c3.size() < 1) continue;
                        localCount++;
                    }
                } // end of f2
            } // end of f4
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
