package cn.edu.nju.pasa.graph.analysis.subgraph.enumerator;

import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.util.HostInfoUtil;

import java.util.Properties;

/**
 * Created by bsidb on 12/5/2017.
 */
public class DoNothingEnumerator implements EnumerateOnCenterVertexTaskInterface {
    @Override
    public void prepare(Properties conf) {

    }

    @Override
    public VertexTaskStats enumerate(int centerVid, VertexCentricAnalysisTask adj) {
        VertexTaskStats stats = new VertexTaskStats(centerVid, adj.getAdj().length,
                -1, 0, 0,
                HostInfoUtil.getHostName());
        return stats;
    }

    @Override
    public void cleanup() {

    }
}
