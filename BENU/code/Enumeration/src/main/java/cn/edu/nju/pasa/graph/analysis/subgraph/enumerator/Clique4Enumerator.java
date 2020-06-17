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
 * four clique - C4
 * u1 - u4
 * |  X |
 * u2 - u3
 * <p>
 * Final Optimal Execution Plan
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=3
 * C2:=Intersect(A1) | >f1
 * f2:=Foreach(C2)
 * A2:=GetAdj(f2) | SIZE>=3
 * T5:=Intersect(A1,A2) | SIZE>=2
 * C3:=Intersect(T5) | >f2
 * f3:=Foreach(C3)
 * A3:=GetAdj(f3) | SIZE>=3
 * T4:=Intersect(T5,A3)
 * C4:=Intersect(T4) | >f3
 * f4:=Foreach(C4)
 * f:=ReportMatch(f1,f2,f3,f4)
 * Final Optimal Execution Plan - Compression
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=3
 * C2:=Intersect(A1) | >f1
 * f2:=Foreach(C2)
 * A2:=GetAdj(f2) | SIZE>=3
 * T5:=Intersect(A1,A2) | SIZE>=2
 * C3:=Intersect(T5) | >f2
 * f3:=Foreach(C3)
 * A3:=GetAdj(f3) | SIZE>=3
 * T4:=Intersect(T5,A3)
 * C4:=Intersect(T4) | >f3
 * f:=ReportMatch(f1,f2,f3,C4)
 * <p>
 * Created by huweiwei on 6/27/2018.
 */

public class Clique4Enumerator implements EnumerateOnCenterVertexTaskInterface {
    AbstractSimpleGraphStorage graphStorage;
    private boolean enumerate;
    private boolean loadBalance;

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

        IntArrayList t4 = new IntArrayList();
        IntArrayList t5 = new IntArrayList();
        IntArrayList c3 = new IntArrayList();
        IntArrayList c4 = new IntArrayList();

        long localCount = 0L;
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, localCount, 0,
                HostInfoUtil.getHostName());
        long t0 = System.nanoTime();
        try {

            // * f1=Init(start)
            int f1 = startVid;
            // * A1=GetAdj(f1) | SIZE>=3
            int a1[] = graphStorage.get(f1);
            if (a1.length < 3) return stats;
            // * C2=Intersect(A1) | >f1
            IntArrayList c2 = new IntArrayList();
            if (loadBalance) {
                int start = task.getStart();
                int end = task.getEnd();
                int taskC2[] = Arrays.copyOfRange(task.getAdj(), start, end + 1);
                Arrays.sort(taskC2);
                CommonFunctions.filterAdjListAdaptive(c2, taskC2, f1);
            } else {
                CommonFunctions.filterAdjListAdaptive(c2, a1, f1);
            }
            if (c2.size() < 1) return stats;
            // * f2=Foreach(C2)
            for (int f2 : c2) {
                // * A2=GetAdj(f2) | SIZE>=3
                int a2[] = graphStorage.get(f2);
                if (a2.length < 3) continue;
                // * T5=Intersect(A1,A2) | SIZE>=2
                CommonFunctions.intersectTwoSortedArrayAdaptive(t5, a1, a2);
                if (t5.size() < 2) continue;
                // * C3=Intersect(T5) | >f1,>f2
                CommonFunctions.filterAdjListAdaptive(c3, t5, f2);
                if (c3.size() < 1) continue;
                // * f3=Foreach(C3)
                for (int f3 : c3) {
                    // * A3=GetAdj(f3) | SIZE>=3
                    int a3[] = graphStorage.get(f3);
                    if (a3.length < 3) continue;
                    // * T4=Intersect(T5,A3)
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t4, t5, a3);
                    if (t4.size() < 1) continue;
                    // * C4=Intersect(T4) | >f1,>f2,>f3
                    CommonFunctions.filterAdjListAdaptive(c4, t4, f3);
                    if (c4.size() < 1) continue;
                    if (enumerate) {
                        for (int f4 : c4)
                            localCount++;
                        // * f4=Foreach(C4)
                        // * F=ReportEmbedding(f1,f2,f3,f4)
                    } else {
                        localCount++;
                    }
                }
            }
        } catch (Exception e) {
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
