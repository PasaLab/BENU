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
 * <p>
 *   u5
 * /   \
 * u1 - u4
 * |    |
 * u2 - u3
 * <p>
 * u1 < u4
 * u1 is the start vertex
 * <p>
 * Execution Plan
 * <p>
 Final Optimal Execution Plan
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=3
 C4:=Intersect(A1) | >f1
 f4:=Foreach(C4)
 C2:=Intersect(A1) | ≠f4
 A4:=GetAdj(f4) | SIZE>=3
 T5:=Intersect(A1,A4)
 f2:=Foreach(C2)
 A2:=GetAdj(f2) | SIZE>=2
 T3:=Intersect(A2,A4) | SIZE>=2
 C3:=Intersect(T3) | ≠f1
 f3:=Foreach(C3)
 C5:=Intersect(T5) | ≠f2,≠f3
 f5:=Foreach(C5)
 f:=ReportMatch(f1,f2,f3,f4,f5)
 Final Optimal Execution Plan - Compression
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=3
 C4:=Intersect(A1) | >f1
 f4:=Foreach(C4)
 C2:=Intersect(A1) | ≠f4
 A4:=GetAdj(f4) | SIZE>=3
 T5:=Intersect(A1,A4)
 f2:=Foreach(C2)
 A2:=GetAdj(f2) | SIZE>=2
 T3:=Intersect(A2,A4) | SIZE>=2
 C3:=Intersect(T3) | ≠f1
 C5:=Intersect(T5) | ≠f2
 f:=ReportMatch(f1,f4,f2,C3,C5)
 * <p>
 * <p>
 * Created by huweiwei on 4/15/2018.
 */

public class HouseEnumerator implements EnumerateOnCenterVertexTaskInterface {
    AbstractSimpleGraphStorage graphStorage;
    private boolean enumerate;
    private boolean loadBalance;

    @Override
    public void prepare(Properties conf) {
        try {
            graphStorage = CacheBasedDistributedGraphStorage.getProcessLevelStorage();
            enumerate = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_ENUMERATE, "false"));
            loadBalance = Boolean.parseBoolean(conf.getProperty(MyConf.ENABLE_LOAD_BALANCE, "false"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public VertexTaskStats enumerate(int centerVid, VertexCentricAnalysisTask task) {

        IntArrayList c4 = new IntArrayList();
        IntArrayList c2 = new IntArrayList();
        IntArrayList t5 = new IntArrayList();
        IntArrayList t3 = new IntArrayList();
        IntArrayList c3 = new IntArrayList();
        IntArrayList c5 = new IntArrayList();

        long localCount = 0L;
        VertexTaskStats stats = new VertexTaskStats(centerVid, task.getAdj().length,
                -1, 0, 0,
                HostInfoUtil.getHostName());
        int f1 = centerVid;
        long t0 = System.nanoTime();
        try {
            int a1[] = graphStorage.get(f1);
            if (a1.length < 3) return stats;
            if (loadBalance) {
                int taskA1[] = Arrays.copyOfRange(task.getAdj(), task.getStart(), task.getEnd() + 1);
                Arrays.sort(taskA1);
                CommonFunctions.filterAdjListAdaptive(c4, taskA1, f1);//C4: >f1
            } else {
                CommonFunctions.filterAdjListAdaptive(c4, a1, f1);//C4: >f1
            }
            for (int f4: c4) {
                c2.clear();
                for (int x : a1) if (x != f4) c2.add(x);
                if (c2.size() < 1) continue;
                int a4[] = graphStorage.get(f4);
                if (a4.length < 3) continue;
                CommonFunctions.intersectTwoSortedArrayAdaptive(t5, a1, a4);
                if (t5.size() < 1) continue;
                for (int f2: c2) {
                    int a2[] = graphStorage.get(f2);
                    if (a2.length < 2) continue;
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t3, a2, a4);
                    if (t3.size() < 2) continue;
                    c3.clear();
                    for (int x: t3) if (x != f1) c3.add(x);
                    if (c3.size() < 1) continue;
                    if (enumerate) {
                        for (int f3: c3) {
                            c5.clear();
                            for (int x: t5) if (x != f2 && x != f3) c5.add(x);
                            for (int f5: c5) {
                                localCount++;
                            }
                        }
                    } else {
                        c5.clear();
                        for (int x: t5) if (x != f2) c5.add(x);
                        if (c5.size() < 1) continue;
                        localCount++;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        long t1 = System.nanoTime();
        stats = new VertexTaskStats(centerVid, task.getAdj().length,
                -1, localCount, (t1 - t0),
                HostInfoUtil.getHostName());
        return stats;
    }

    @Override
    public void cleanup() {

    }
}