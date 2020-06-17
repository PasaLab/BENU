package cn.edu.nju.pasa.graph.analysis.subgraph.enumerator;

import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;

import java.util.Properties;

/**
 * Enumerate embeddings on a center data vertex.
 * Created by Zhaokang Wang on 12/5/2017.
 */
public interface EnumerateOnCenterVertexTaskInterface {
    /**
     * Do some preparation work.
     * @param conf job configuration
     */
    public void prepare(Properties conf);

    /**
     * Enumerate the embeddings on the center vid
     * This method must be thread-safe!
     * @param centerVid the id of the center vertex
     * @param adj the adjacency list info of the center vertex
     * @exception Exception any exception relates to the database access
     * @return the statistics of the enumeration
     */
    VertexTaskStats enumerate(int centerVid, VertexCentricAnalysisTask adj) throws Exception;

    /**
     * Do some cleanup work.
     */
    public void cleanup();
}
