package cn.edu.nju.pasa.graph.plangen;

import java.util.*;

/**
 * A simple implementation of directed graph.
 * Created by huweiwei on 6/14/18.
 */
public class DiGraph {
    private Map<String, List<String>> graph = new HashMap<String, List<String>>(); // v: N(v)
    private Map<String, Integer> mark = new HashMap<String, Integer>(); // v: mark
    private Map<String, List<Integer>> condition = new HashMap<String, List<Integer>>();
    // v: automorphism symmetry-breaking condition eg.u0: <u1,<u2,>u3,>u4 stored as 0: -1,-2,+3,+4

    public void addEdge(String v1, String v2) {
        List<String> v1Adj = graph.getOrDefault(v1, new LinkedList<String>());
        mark.put(v1, 0);
        if(!v1Adj.contains(v2)) {
            v1Adj.add(v2);
            Collections.sort(v1Adj);
            graph.put(v1, v1Adj);
        }

        List<String> v2Adj;
        if (!graph.containsKey(v2)) {
            mark.put(v2, 0);
            v2Adj = new LinkedList<String>();
            graph.put(v2, v2Adj);
        }
    }

    public void addCondition(String target, List<Integer> c) {
        List<Integer> cond;
        if(condition.containsKey(target)) {
            cond = condition.get(target);
            cond.addAll(c);
        } else {
            cond = new ArrayList<Integer>();
            cond.addAll(c);
            condition.put(target, cond);
        }

    }

    public void removeCondition(String target, List<Integer> c) {
        List<Integer> cond;
        if(condition.containsKey(target)) {
            cond = condition.get(target);
            cond.removeAll(c);
        }
    }

    public List<String> topsort() {
        initVertexMark();
        List<String> topSequence = new ArrayList<String>();
        for(Map.Entry<String, List<String>> entry : graph.entrySet()) {
            String key = entry.getKey();
            if(mark.get(key) == 0) {
                tophelp(key, topSequence);
            }
        }
        return topSequence;
    }

    private void tophelp(String v, List<String> topSequence) {
        mark.put(v, 1);
        List<String> adj = graph.get(v);
        for(String w: adj) {
            if(mark.get(w) == 0) {
                tophelp(w, topSequence);
            }
        }
        //System.out.print(v + ",");
        topSequence.add(v);
    }

    /**
     * Set all vertex unvisited.
     */
    public void initVertexMark() {
        Iterator<String> iter = mark.keySet().iterator();
        while(iter.hasNext()) {
            String key = iter.next();
            mark.put(key, 0);
        }
    }

    /**
     * Get the adj list of v.
     */
    public List<String> getAdj(String v) {
        return graph.get(v);
    }

    public List<Integer> getCondition(String v) {
        return condition.get(v);
    }

    public void print() {
        for(Map.Entry<String, List<String>> entry : graph.entrySet()) {
            String key = entry.getKey();
            List<String> value = entry.getValue();
            System.out.print(key + ": ");
            for(String val: value) {
                System.out.print(val + ",");
            }
            if(condition.containsKey(key)) {
                System.out.print(" | ");
                for (Integer c : condition.get(key)) {
                    System.out.print(c + ",");
                }
            }
            System.out.println();
        }
    }
}
