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
 * Pattern Graph: StarofDavidPlus
 * <p>
 * Execution Plan
 * <p>
 Final Optimal Execution Plan
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=5
 C4:=Intersect(A1) | >f1
 f4:=Foreach(C4)
 A4:=GetAdj(f4) | SIZE>=5
 T8:=Intersect(A1,A4) | SIZE>=4
 C2:=Intersect(T8)
 f2:=Foreach(C2)
 C5:=Intersect(T8) | >f2
 A2:=GetAdj(f2) | SIZE>=4
 T9:=Intersect(T8,A2) | SIZE>=2
 f5:=Foreach(C5)
 A5:=GetAdj(f5) | SIZE>=4
 T7:=Intersect(T9,A5) | SIZE>=2
 C3:=Intersect(T7) | >f2
 f3:=Foreach(C3)
 C6:=Intersect(T7) | >f3
 f6:=Foreach(C6)
 f:=ReportMatch(f1,f2,f3,f4,f5,f6)
 Final Optimal Execution Plan - Compression
 ---------------------------------------------------
 f1:=Init(start)
 A1:=GetAdj(f1) | SIZE>=5
 C4:=Intersect(A1) | >f1
 f4:=Foreach(C4)
 A4:=GetAdj(f4) | SIZE>=5
 T8:=Intersect(A1,A4) | SIZE>=4
 C2:=Intersect(T8)
 f2:=Foreach(C2)
 C5:=Intersect(T8) | >f2
 A2:=GetAdj(f2) | SIZE>=4
 T9:=Intersect(T8,A2) | SIZE>=2
 f5:=Foreach(C5)
 A5:=GetAdj(f5) | SIZE>=4
 T7:=Intersect(T9,A5) | SIZE>=2
 C3:=Intersect(T7) | >f2
 C6:=Intersect(T7) | >f2
 f:=ReportMatch(f1,f4,f2,f5,C3,C6)
 * <p>
 * Created by wzk on 9/20/2018.
 */

public class StarOfDavidPlusEnumerator implements EnumerateOnCenterVertexTaskInterface {
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
            //f1=Init(start)
            int f1 = startVid;
            //A1=GetAdj(f1) | SIZE>=5
            int a1[] = graphStorage.get(f1);
            if (a1.length < 5) return stats;
            //C4=Intersect(A1) | >f1
            IntArrayList c4 = new IntArrayList();
            if (enableLoadBanlance) {
                int a1BalandedPart[] = Arrays.copyOfRange(task.getAdj(), task.getStart(), task.getEnd() + 1);
                Arrays.sort(a1BalandedPart);
                CommonFunctions.filterAdjListAdaptive(c4, a1BalandedPart, f1);
            } else {
                CommonFunctions.filterAdjListAdaptive(c4, a1, f1);
            }
            //f4=Foreach(C4)
            for(int f4: c4) {
                //A4=GetAdj(f4) | SIZE>=5
                int[] a4 = graphStorage.get(f4);
                if (a4.length < 5) continue;
                //T8=Intersect(A1,A4) | SIZE>=4
                IntArrayList t8 = new IntArrayList();
                CommonFunctions.intersectTwoSortedArrayAdaptive(t8, a1, a4);
                if (t8.size() < 4) continue;
                //C2=Intersect(T8)
                IntArrayList c2 = t8;
                //f2=Foreach(C2)
                for(int f2: c2) {
                    //C5=Intersect(T8) | >f2
                    IntArrayList c5 = new IntArrayList();
                    CommonFunctions.filterAdjListAdaptive(c5, t8, f2);
                    if (c5.size() < 1) continue;
                    //A2=GetAdj(f2) | SIZE>=4
                    int a2[] = graphStorage.get(f2);
                    if (a2.length < 4) continue;
                    //T9=Intersect(T8,A2) | SIZE>=2
                    IntArrayList t9 = new IntArrayList();
                    CommonFunctions.intersectTwoSortedArrayAdaptive(t9, t8, a2);
                    if (t9.size() < 2) continue;
                    //f5=Foreach(C5)
                    for(int f5: c5) {
                        //A5=GetAdj(f5) | SIZE>=4
                        int a5[] = graphStorage.get(f5);
                        if (a5.length < 4) continue;
                        //T7=Intersect(T9,A5) | SIZE>=2
                        IntArrayList t7 = new IntArrayList();
                        CommonFunctions.intersectTwoSortedArrayAdaptive(t7, t9, a5);
                        if (t7.size() < 2) continue;
                        //C3=Intersect(T7) | >f2
                        IntArrayList c3 = new IntArrayList();
                        CommonFunctions.filterAdjListAdaptive(c3, t7, f2);
                        if (enumerate) {
                            //f3=Foreach(C3)
                            for(int f3: c3) {
                                //C6=Intersect(T7) | >f2, >f3
                                IntArrayList c6 = new IntArrayList();
                                CommonFunctions.filterAdjList(c6, c3, f3);
                                //f6=Foreach(C6)
                                for (int f6: c6) {
                                    //F=ReportEmbedding(f1,f2,f3,f4,f5,f6)
                                    localCount++;
                                }
                            }
                        } else {
                            IntArrayList c6 = c3;
                            localCount++;
                        } // end of enumerate
                    } // end of f6
                } // end of f2
            } // end of f4
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