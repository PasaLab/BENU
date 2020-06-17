package cn.edu.nju.pasa.graph.plangen.incremental;

import java.util.*;

/**
 * A simple implementation of directed graph.
 * Created by huweiwei on 6/24/18.
 */
class Edge {
    private int start;
    private int end;

    public Edge(int v1, int v2) {
        start = v1;
        end = v2;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    @Override
    public int hashCode() {
        return (((Integer)start).hashCode() + ((Integer)end).hashCode()) * 17;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Edge) {
            Edge edge = (Edge) obj;
            return (this.start == edge.start && this.end == edge.end);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Edge(" + start + "," + end + ")";
    }
}

public class DiGraph {
    private Map<Integer, List<Integer>> graph = new HashMap<Integer, List<Integer>>(); // v: N(v)
    private Map<Integer, Integer> vertexMark = new HashMap<Integer, Integer>(); // v: vertexMark
    private Map<Edge, Integer> edgeMark = new HashMap<>(); // edge: edgeMark
    private Map<Integer, List<Integer>> symmetryBreakingCondition = new HashMap<>();
    // v: automorphism symmetry-breaking condition eg.u1: <u2,<u3,>u4,>u5 stored as 1: -2,-3,+4,+5

    public void addEdge(int v1, int v2) {
        List<Integer> v1Adj = graph.getOrDefault(v1, new LinkedList<>());
        vertexMark.put(v1, 0);
        if(!v1Adj.contains(v2)) {
            v1Adj.add(v2);
            Collections.sort(v1Adj);
            graph.put(v1, v1Adj);
        }

        List<Integer> v2Adj;
        if (!graph.containsKey(v2)) {
            vertexMark.put(v2, 0);
            v2Adj = new LinkedList<>();
            graph.put(v2, v2Adj);
        }

        edgeMark.put(new Edge(v1, v2), 0);
    }

    /**
     * Add symmetry breaking condition between v1 and v2, the condition is v1 < v2
     */
    public void addCondition(int v1, int v2) {
        List<Integer> cond1 = symmetryBreakingCondition.computeIfAbsent(v1, k -> new ArrayList<>());
        cond1.add(-v2); // v1 < v2
        List<Integer> cond2 = symmetryBreakingCondition.computeIfAbsent(v2, k -> new ArrayList<>());
        cond1.add(v1); // v2 > v1
    }

    /**
     * Get all nodes in graph.
     */
    public List<Integer> getAllVertices() {
        List<Integer> vertices = new ArrayList<>(graph.keySet());
        return vertices;
    }

    /**
     * Get the in and out degree of v.
     */
    public int getInAndOutDegree(int v) {
        int degree = 0;
        degree += getInDegree(v);
        degree += getOutDegree(v);
        return degree;
    }

    /**
     * Get the in and out degree of v.
     * The in_degree is the number of edges pointing to v.
     */
    public int getInDegree(int v) {
        int degree = 0;
        if(graph.containsKey(v)) {
            for(Map.Entry<Integer, List<Integer>> entry : graph.entrySet()) {
                int key = entry.getKey();
                List<Integer> value = entry.getValue();
                if(key != v && value.contains(v)) degree++;
            }
        }
        return degree;
    }

    public int getOutDegree(int v) {
        if(graph.containsKey(v)) {
            return graph.get(v).size();
        }
        else {
            return 0;
        }
    }

    /**
     * Get the in and out adj list of v.
     */
    public List<Integer> getInAndOutAdj(int v) {
        List<Integer> adj = new ArrayList<>();
        adj.addAll(getInAdj(v)); // add in adj
        adj.addAll(getOutAdj(v)); // add out adj
        Collections.sort(adj);
        return adj;
    }

    public List<Integer> getInAdj(int v) {
        List<Integer> adj = new ArrayList<>();
        for(Map.Entry<Integer, List<Integer>> entry : graph.entrySet()) {
            int key = entry.getKey();
            List<Integer> value = entry.getValue();
            if(key != v && value.contains(v)) {
                adj.add(key);
            }
        }
        Collections.sort(adj);
        return adj;
    }

    public List<Integer> getOutAdj(int v) {
        return new ArrayList<>(graph.get(v));
    }

    public int getVertexNum() {
        return vertexMark.size();
    }

    public List<Integer> getCondition(int v) {
        return symmetryBreakingCondition.get(v);
    }

    /**
     * Set all edge uncovered.
     */
    public void initEdgeMark() {
        Iterator<Edge> iter = edgeMark.keySet().iterator();
        while(iter.hasNext()) {
            Edge key = iter.next();
            edgeMark.put(key, 0);
        }
    }

    /**
     * Mark the in and out edges of v covered.
     */
    public void markCoveredEdges(int v) {
        for(Map.Entry<Integer, List<Integer>> entry : graph.entrySet()) {
            int key = entry.getKey();
            List<Integer> value = entry.getValue();
            if (key == v) {
                for (int adj : value) {
                    edgeMark.put(new Edge(v, adj), 1);
                }
            } else if (value.contains(v)) {
                edgeMark.put(new Edge(key, v), 1);
            }
        }
    }

    public boolean isGraphCovered() {
        if(edgeMark.values().contains(0)) return false;
        return true;
    }
}
