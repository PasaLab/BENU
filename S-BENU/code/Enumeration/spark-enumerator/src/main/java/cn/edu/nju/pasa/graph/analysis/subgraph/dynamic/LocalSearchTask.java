package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import java.io.Serializable;

/**
 * Local search task for a vertex and an incremental execution plan
 */
final public class LocalSearchTask implements Serializable {
    private int centerVid;
    private int deltaAdj[];
    private int incrementalExecutionPlanIndex;

    public LocalSearchTask(int centerVid, int[] deltaAdj, int incrementalExecutionPlanIndex) {
        this.centerVid = centerVid;
        this.deltaAdj = deltaAdj;
        this.incrementalExecutionPlanIndex = incrementalExecutionPlanIndex;
    }

    public int getCenterVid() {
        return centerVid;
    }

    public int[] getDeltaAdj() {
        return deltaAdj;
    }

    public int getIncrementalExecutionPlanIndex() {
        return incrementalExecutionPlanIndex;
    }
}
