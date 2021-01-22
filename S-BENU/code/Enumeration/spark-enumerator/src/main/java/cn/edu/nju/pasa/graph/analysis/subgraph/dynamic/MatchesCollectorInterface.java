package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import java.util.List;

/**
 * Collect matches.
 * The `offer` method should be thread safe.
 *
 * Create by Weiwei Hu on 12/30/2019
 */
public interface MatchesCollectorInterface {
    public void offer(int[] match, int flag);
    public long getDeltaCount();
    public long getCounteractCount();
    public void cleanup();
}
