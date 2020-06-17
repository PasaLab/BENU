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
 * five clique - C5
 *    u5
 *
 * u1 - u4
 * |  X |
 * u2 - u3
 *
 * Final Optimal Execution Plan
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=4
 * C2:=Intersect(A1) | >f1
 * f2:=Foreach(C2)
 * A2:=GetAdj(f2) | SIZE>=4
 * T7:=Intersect(A1,A2) | SIZE>=3
 * C3:=Intersect(T7) | >f2
 * f3:=Foreach(C3)
 * A3:=GetAdj(f3) | SIZE>=4
 * T6:=Intersect(A3,T7) | SIZE>=2
 * C4:=Intersect(T6) | >f3
 * f4:=Foreach(C4)
 * A4:=GetAdj(f4) | SIZE>=4
 * T5:=Intersect(T6,A4)
 * C5:=Intersect(T5) | >f4
 * f5:=Foreach(C5)
 * f:=ReportMatch(f1,f2,f3,f4,f5)
 * Final Optimal Execution Plan - Compression
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=4
 * C2:=Intersect(A1) | >f1
 * f2:=Foreach(C2)
 * A2:=GetAdj(f2) | SIZE>=4
 * T7:=Intersect(A1,A2) | SIZE>=3
 * C3:=Intersect(T7) | >f2
 * f3:=Foreach(C3)
 * A3:=GetAdj(f3) | SIZE>=4
 * T6:=Intersect(A3,T7) | SIZE>=2
 * C4:=Intersect(T6) | >f3
 * f4:=Foreach(C4)
 * A4:=GetAdj(f4) | SIZE>=4
 * T5:=Intersect(T6,A4)
 * C5:=Intersect(T5) | >f4
 * f:=ReportMatch(f1,f2,f3,f4,C5)
 *
 * Created by huweiwei on 6/27/2018.
 */

public class Clique5Enumerator implements EnumerateOnCenterVertexTaskInterface {
    AbstractSimpleGraphStorage graphStorage;
    private boolean enumerate, loadBalance;
    @Override
    public void prepare(Properties conf) {
        try {
            graphStorage = CacheBasedDistributedGraphStorage.getProcessLevelStorage();
            enumerate = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_ENUMERATE, "false"));
            loadBalance = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_LOAD_BALANCE, "true"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public VertexTaskStats enumerate(int startVid, VertexCentricAnalysisTask task) {
        IntArrayList t5 = new IntArrayList();
        IntArrayList t6 = new IntArrayList();
        IntArrayList t7 = new IntArrayList();
        IntArrayList c2 = new IntArrayList();
        IntArrayList c3 = new IntArrayList();
        IntArrayList c4 = new IntArrayList();
        IntArrayList c5 = new IntArrayList();

        long localCount = 0L;
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, localCount, 0,
                HostInfoUtil.getHostName());
        long t0 = System.nanoTime();
        try {

            // * f1=Init(start)
            int f1 = startVid;
            // * A1=GetAdj(f1) | SIZE>=4
            int a1[] = graphStorage.get(f1);
            if (a1.length < 4) return stats;
            // * C2=Intersect(A1) | >f1
            if (loadBalance) {
                int start = task.getStart();
                int end = task.getEnd();
                int taskC2[] = Arrays.copyOfRange(task.getAdj(), start, end  + 1);
                Arrays.sort(taskC2);
                CommonFunctions.filterAdjListAdaptive(c2, taskC2, f1);
            } else {
                CommonFunctions.filterAdjListAdaptive(c2, a1, f1);
            }
            if (c2.size() < 1) return stats;
            // * f2=Foreach(C2)
            for (int f2: c2) {
                // * A2=GetAdj(f2) | SIZE>=4
                int a2[] = graphStorage.get(f2);
                if (a2.length < 4) continue;
                // * T7=Intersect(A1,A2) | SIZE>=3
                CommonFunctions.intersectTwoSortedArrayAdaptive(t7, a1, a2);
                if (t7.size() < 3) continue;
                // * C3=Intersect(T7) | >f1,>f2
                CommonFunctions.filterAdjListAdaptive(c3, t7, f2);
                if (c3.size() < 1) continue;
                // * f3=Foreach(C3)
                for(int f3: c3) {
                    // * A3=GetAdj(f3) | SIZE>=4
                    int a3[] = graphStorage.get(f3);
                    if (a3.length < 4) continue;
                    // * T6=Intersect(A3,T7) | SIZE>=2
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t6, t7, a3);
                    if (t6.size() < 1) continue;
                    // * C4=Intersect(T6) | >f1,>f2,>f3
                    CommonFunctions.filterAdjListAdaptive(c4, t6, f3);
                    if (c4.size() < 1) continue;
                    // * f4=Foreach(C4)
                    for (int f4: c4) {
                        // * A4=GetAdj(f4) | SIZE>=4
                        int a4[] = graphStorage.get(f4);
                        if (a4.length < 4) continue;
                        // * T5=Intersect(T6,A4)
                        CommonFunctions.intersectTwoSortedArrayAdaptive(t5, t6, a4);
                        if (t5.size()  < 1) continue;
                        // * C5=Intersect(T5) | >f1,>f2,>f3,>f4
                        CommonFunctions.filterAdjListAdaptive(c5, t5, f4);
                        if (c5.size() < 1) continue;
                        if (enumerate) {
                            // * f5=Foreach(C5)
                            // * F=ReportEmbedding(f1,f2,f3,f4,f5)
                            for (int f5: c5)
                                localCount++;
                        } else {
                            localCount++;
                        } // enumerate
                    } // f4
                } // f3
            } // f2
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
