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
 * three clique - C3
 *    u1
 *  /   \
 * u2 - u3
 *
 * Final Optimal Execution Plan
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=2
 * C2:=Intersect(A1) | >f1
 * f2:=Foreach(C2)
 * A2:=GetAdj(f2) | SIZE>=2
 * T3:=Intersect(A1,A2)
 * C3:=Intersect(T3) | >f2
 * f3:=Foreach(C3)
 * f:=ReportMatch(f1,f2,f3)
 * Final Optimal Execution Plan - Compression
 * ---------------------------------------------------
 * f1:=Init(start)
 * A1:=GetAdj(f1) | SIZE>=2
 * C2:=Intersect(A1) | >f1
 * f2:=Foreach(C2)
 * A2:=GetAdj(f2) | SIZE>=2
 * T3:=Intersect(A1,A2)
 * C3:=Intersect(T3) | >f2
 * f:=ReportMatch(f1,f2,C3)
 *
 * Created by huweiwei on 6/27/2018.
 */

public class Clique3Enumerator implements EnumerateOnCenterVertexTaskInterface {
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
        long localCount = 0L;
        VertexTaskStats stats = new VertexTaskStats(startVid, task.getAdj().length,
                -1, 0L, 0L,
                HostInfoUtil.getHostName());

        long t0 = System.nanoTime();
        IntArrayList c2 = new IntArrayList();
        IntArrayList t3 = new IntArrayList();
        IntArrayList c3 = new IntArrayList();
        try {
            int f1 = startVid;
            int a1[] = graphStorage.get(f1);
            if (a1.length  < 2) return stats;
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
            for (int f2: c2) {
                int a2[] = graphStorage.get(f2);
                if (a2.length < 2) continue;
                CommonFunctions.intersectTwoSortedArrayAdaptive(t3, a1, a2);
                if (t3.size() < 1) continue;
                CommonFunctions.filterAdjListAdaptive(c3, t3, f2);
                if (c3.size() < 1) continue;
                if (enumerate) {
                    for (int f3: c3)
                        localCount++;
                } else {
                    localCount++;
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
