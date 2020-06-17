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
 * Pattern Graph: TwinClique4
 * <p>
 * Execution Plan
 * <p>
 Final Optimal Execution Plan
 ---------------------------------------------------
 f3:=Init(start)
 A3:=GetAdj(f3) | SIZE>=5
 C6:=Intersect(A3) | >f3
 f6:=Foreach(C6)
 A6:=GetAdj(f6) | SIZE>=5
 T7:=Intersect(A3,A6) | SIZE>=4
 C1:=Intersect(T7)
 f1:=Foreach(C1)
 C4:=Intersect(T7) | >f1
 A1:=GetAdj(f1) | SIZE>=3
 T2:=Intersect(A1,T7)
 f4:=Foreach(C4)
 C2:=Intersect(T2) | >f1,≠f4
 A4:=GetAdj(f4) | SIZE>=3
 T5:=Intersect(A4,T7)
 f2:=Foreach(C2)
 C5:=Intersect(T5) | >f4,≠f2
 f5:=Foreach(C5)
 f:=ReportMatch(f1,f2,f3,f4,f5,f6)
 Final Optimal Execution Plan - Compression
 ---------------------------------------------------
 f3:=Init(start)
 A3:=GetAdj(f3) | SIZE>=5
 C6:=Intersect(A3) | >f3
 f6:=Foreach(C6)
 A6:=GetAdj(f6) | SIZE>=5
 T7:=Intersect(A3,A6) | SIZE>=4
 C1:=Intersect(T7)
 f1:=Foreach(C1)
 C4:=Intersect(T7) | >f1
 A1:=GetAdj(f1) | SIZE>=3
 T2:=Intersect(A1,T7)
 f4:=Foreach(C4)
 C2:=Intersect(T2) | >f1,≠f4
 A4:=GetAdj(f4) | SIZE>=3
 T5:=Intersect(A4,T7)
 C5:=Intersect(T5) | >f4
 f:=ReportMatch(f3,f6,f1,f4,C2,C5)
 * <p>
 * Created by wzk on 9/23/2018.
 */

public class TwinClique4Enumerator implements EnumerateOnCenterVertexTaskInterface {
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
        long t0 = System.nanoTime();
        try {
            // f3=Init(start)
            int f3 = startVid;
            // A3=GetAdj(f3) | SIZE>=5
            int a3[] = graphStorage.get(f3);
            if (a3.length < 5) return stats;
            // C6=Intersect(A3) | >f3
            IntArrayList c6 = new IntArrayList();
            if (enableLoadBanlance) {
                int a3BalancedAdj[] = Arrays.copyOfRange(task.getAdj(), task.getStart(), task.getEnd()+1);
                Arrays.sort(a3BalancedAdj);
                CommonFunctions.filterAdjListAdaptive(c6, a3BalancedAdj, f3);
            } else {
                CommonFunctions.filterAdjListAdaptive(c6, a3, f3);
            }
            if (c6.size() < 1) return stats;
            // f6=Foreach(C6)
            for(int f6: c6) {
                // A6=GetAdj(f6) | SIZE>=5
                int a6[] = graphStorage.get(f6);
                if (a6.length < 5) continue;
                // T7=Intersect(A3,A6) | SIZE>=4
                IntArrayList t7 = new IntArrayList();
                CommonFunctions.intersectTwoSortedArrayAdaptive(t7, a3, a6);
                if (t7.size() < 4) continue;
                // C1=Intersect(T7)
                IntArrayList c1 = t7;
                // f1=Foreach(C1)
                for (int f1: c1) {
                    // C4=Intersect(T7) | >f1
                    IntArrayList c4 = new IntArrayList();
                    CommonFunctions.filterAdjListAdaptive(c4, t7, f1);
                    if (c4.size() < 1) continue;
                    // A1=GetAdj(f1) | SIZE>=3
                    int a1[] = graphStorage.get(f1);
                    if (a1.length < 3) continue;
                    // T2=Intersect(A1,T7)
                    IntArrayList t2 = new IntArrayList();
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t2, t7, a1);
                    if (t2.size() < 1) continue;
                    // f4=Foreach(C4)
                    for (int f4: c4) {
                        // C2=Intersect(T2) | >f1,≠f4
                        IntArrayList c2 = new IntArrayList();
                        for(int x: t2) if (x > f1 && x != f4) c2.add(x);
                        if (c2.size() < 1) continue;
                        // A4=GetAdj(f4) | SIZE>=3
                        int a4[] = graphStorage.get(f4);
                        if (a4.length < 3) continue;
                        // T5=Intersect(A4,T7)
                        IntArrayList t5 = new IntArrayList();
                        CommonFunctions.intersectTwoSortedArrayAdaptive(t5, t7, a4);
                        if (t5.size() < 1) continue;
                        if (enumerate) {
                            // TODO:
                            //f2=Foreach(C2)
                            // C5=Intersect(T5) | >f1,>f4,≠f2
                            // f5=Foreach(C5)
                            // F=ReportEmbedding(f1,f2,f3,f4,f5,f6)
                            for(int f2: c2) {
                                IntArrayList c5 = new IntArrayList();
                                for (int x : t5) if (x > f1 && x > f4 && x != f2) c5.add(x);
                                for (int f5: c5) localCount++;
                            }
                        } else {
                            /*
                            // C5=Intersect(T5) | >f1,>f4
                            IntArrayList c5 = new IntArrayList();
                            for (int x : t5) if (x > f1 && x > f4) c5.add(x);
                            if (c5.size() < 1) continue;
                            */
                            /*
                            int c5[] = t5.toIntArray();
                            int x = (f1 >= f4) ? f1 : f4;
                            int pos = Arrays.binarySearch(c5, x);
                            if (pos >= 0 && pos < (c5.length - 1)) localCount++;
                            */
                            IntArrayList c5 = new IntArrayList();
                            int x = (f1 >= f4) ? f1 : f4;
                            CommonFunctions.filterAdjListAdaptive(c5, t5, x);
                            if (c5.size() < 1) continue;
                            localCount++;
                        } // end of enumerate
                    } // end of f4
                } // end of f1
            } // end of f6
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