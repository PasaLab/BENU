package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CountCollector implements MatchesCollectorInterface {
    private AtomicLong deltaCount = new AtomicLong(0L);
    private AtomicLong counteractCount = new AtomicLong(0L);
    @Override
    public void offer(int[] match, int op) {
        deltaCount.addAndGet(1L);
        counteractCount.addAndGet((long)op);
    }

    @Override
    public long getDeltaCount() {
        return deltaCount.get();
    }

    @Override
    public long getCounteractCount() {
        return counteractCount.get();
    }

    @Override
    public void cleanup() {

    }
}
