package cn.edu.nju.pasa.graph.plangen;

import java.util.*;

/**
 * A simple implementation of undirected graph.
 * Created by huweiwei on 5/23/18.
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
            return ((this.start == edge.start && this.end == edge.end) ||
                    this.start == edge.end && this.end == edge.start);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Edge(" + start + "," + end + ")";
    }
}

public class Graph {
    private Map<Integer, List<Integer>> graph = new HashMap<Integer, List<Integer>>(); // v: N(v)
    private Map<Integer, Integer> vertexMark = new HashMap<Integer, Integer>(); // v: vertexMark
    private Map<Edge, Integer> edgeMark = new HashMap<>(); // edge: edgeMark

    public void addEdge(int v1, int v2) {
        List<Integer> v1Adj = graph.getOrDefault(v1, new LinkedList<>());
        vertexMark.put(v1, 0);
        if(!v1Adj.contains(v2)) {
            v1Adj.add(v2);
            Collections.sort(v1Adj);
            graph.put(v1, v1Adj);
        }

        List<Integer> v2Adj = graph.getOrDefault(v2, new LinkedList<>());
        vertexMark.put(v2, 0);
        if(!v2Adj.contains(v1)) {
            v2Adj.add(v1);
            Collections.sort(v2Adj);
            graph.put(v2, v2Adj);
        }

        edgeMark.put(new Edge(v1, v2), 0);
    }

    public void addEdge(Edge e) {
        int v1 = e.getStart();
        int v2 = e.getEnd();
        addEdge(v1, v2);
    }

    public void addVertex(int v) {
        if(!graph.containsKey(v)) {
            graph.put(v, new LinkedList<Integer>());
            vertexMark.put(v, 0);
        }
    }

    public boolean containsVertex(int v) {
        return vertexMark.containsKey(v);
    }

    /**
     * Get all nodes in graph.
     */
    public List<Integer> getAllVertices() {
        List<Integer> vertices = new ArrayList<>(graph.keySet());
        return vertices;
    }

    /**
     * Get all edges in graph.
     */
    public Set<Edge> getEdgesSet() {
        Set<Edge> edges = new LinkedHashSet<Edge>();
        for(Map.Entry<Integer, List<Integer>> entry : graph.entrySet()) {
            int key = entry.getKey();
            List<Integer> value = entry.getValue();
            for(int val: value) {
                Edge e = new Edge(key, val);
                edges.add(e);
            }
        }
        return edges;
    }

    /**
     * Get dfs edges in graph.
     *
     * @param v        source of dfs
     * @param dfsEdges the reasult is stored in this set
     */
    public void dfs(int v, Set<Edge> dfsEdges) {
        vertexMark.put(v, 1);
        List<Integer> adj = graph.get(v);
        for (int w: adj) {
            if(vertexMark.get(w) == 0) {
                Edge e = new Edge(v, w);
                dfsEdges.add(e);
                dfs(w, dfsEdges);
            }
        }
    }

    /**
     * Get the depth of graph using bfs.
     *
     * @param start source of bfs
     * @param q     the intermediate vertex is stored in this queue
     */
    public int bfs(int start, Queue<Integer> q) {
        q.offer(start);
        vertexMark.put(start, 1);
        while (q.size() != 0) {
            int v = q.poll();
            List<Integer> adj = graph.get(v);
            int parentDepth = vertexMark.get(v);
            for (int w: adj) {
                if(vertexMark.get(w) == 0) {
                    vertexMark.put(w, parentDepth+1);
                    q.offer(w);
                }
            }
        }

        Object[] depth = vertexMark.values().toArray();
        Arrays.sort(depth);
        int d = (Integer)depth[depth.length - 1];
        return d;
    }

    /**
     * Set all vertex unvisited.
     */
    public void initVertexMark() {
        Iterator<Integer> iter = vertexMark.keySet().iterator();
        while(iter.hasNext()) {
            int key = iter.next();
            vertexMark.put(key, 0);
        }
    }

    /**
     * Get the degree of v.
     */
    public int getDegree(int v) {
        if(graph.containsKey(v)) {
            return graph.get(v).size();
        }
        else {
            return 0;
        }
    }

    /**
     * Get the adj list of v.
     */
    public List<Integer> getAdj(int v) {
        return new ArrayList<>(graph.get(v));
    }

    public int getVertexNum() {
        return vertexMark.size();
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
     * Mark the edges of v covered.
     */
    public void markCoveredEdges(int v) {
        for(int adj: graph.get(v)) {
            edgeMark.put(new Edge(v, adj), 1);
        }
    }

    public boolean isGraphCovered() {
        if(edgeMark.values().contains(0)) return false;
        return true;
    }

    /**
     * Remove vertex from graph.
     */
    public void removeVertex(int v) {
        if(containsVertex(v)) {
            List<Integer> adjList = graph.get(v);
            Integer vertex = new Integer(v);
            for (int a : adjList) {
                graph.get(a).remove(vertex); // remove edge(a, vertex)
                edgeMark.remove(a, vertex);
            }
            graph.remove(v);
            vertexMark.remove(v);
        }
    }

    /**
     * Return all connected component of graph.
     */
    public List<Graph> getConnectedComponent() {
        initVertexMark();
        List<Integer> vertices = new ArrayList<>(graph.keySet());
        List<Graph> connectedComponent = new ArrayList<>();
        for(int i = 0;i < vertices.size(); i++) {
            int v = vertices.get(i);
            if(vertexMark.get(v) == 0) {
                Set<Edge> dfsEdges = new HashSet<>();
                dfs(v, dfsEdges);

                // construct connected component
                Graph g = new Graph();
                if(dfsEdges.isEmpty()) {
                    g.addVertex(v);
                } else {
                    for (Edge e : dfsEdges) { // add dfs edges
                        g.addEdge(e);
                    }
                }
                Set<Edge> nonDfsEdges = new HashSet<>(getEdgesSet());
                nonDfsEdges.removeAll(dfsEdges);
                for(Edge e: nonDfsEdges) {
                    int start = e.getStart();
                    int end = e.getEnd();
                    if(g.containsVertex(start) && g.containsVertex(end)) {
                        g.addEdge(e);
                    }
                }
                connectedComponent.add(g);
            }
        }

        return connectedComponent;
    }

    /**
     * Return the number of connected component.
     */
    public int getConnectedComponentCount() {
        initVertexMark();
        List<Integer> vertices = new ArrayList<>(graph.keySet());
        int count = 0;
        for(int i = 0;i < vertices.size(); i++) {
            int v = vertices.get(i);
            if(vertexMark.get(v) == 0) {
                count++;
                dfs(v, new HashSet<>());
            }
        }
        return count;
    }

    public void print() {
        for(Map.Entry<Integer, List<Integer>> entry : graph.entrySet()) {
            int key = entry.getKey();
            List<Integer> value = entry.getValue();
            System.out.print(key + ": ");
            for(int val: value) {
                System.out.print(val);
            }
            System.out.println();
        }
    }
}