package cn.edu.nju.pasa.graph.plangen;

import org.omg.PortableInterceptor.INACTIVE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class PartialPatternMatchesEstimator {
    private Graph partialPattern = new Graph();

    public PartialPatternMatchesEstimator(Graph partialPattern) {
        this.partialPattern = partialPattern;
    }

    public BigInteger computeCost(ArrayList<Integer> degArray, HashMap<Integer, BigDecimal> sumOfPower, int centerVid, long dataGraphSize) {
        //System.out.println("partialPattern: ");
        //partialPattern.print();
        List<Graph> connectedComponent = partialPattern.getConnectedComponent();
        //System.out.println("the number of connected component: " + connectedComponent.size());
        BigDecimal totalMatches = BigDecimal.ONE;
        for(Graph g: connectedComponent) {
            //System.out.println("connected component: ");
            //g.print();
            if(g.getVertexNum() == 1) {
                totalMatches = totalMatches.multiply(BigDecimal.valueOf(dataGraphSize));
                //System.out.println("matches: " + totalMatches);
            } else {
                if (g.containsVertex(centerVid)) {
                    totalMatches = totalMatches.multiply(computeCostInSeed(degArray, sumOfPower, centerVid));
                } else {
                    totalMatches = totalMatches.multiply(computeCostInSeed(degArray, sumOfPower, g.getAllVertices().get(0)));
                }
                //System.out.println("matches: " + totalMatches);
            }
        }
        return totalMatches.toBigInteger();
    }

    public BigDecimal computeCostInSeed(ArrayList<Integer> degArray, HashMap<Integer, BigDecimal> sumOfPower, int centerVid) {
        LinkedHashSet<Edge> dfsEdges = new LinkedHashSet<Edge>();;
        partialPattern.initVertexMark();
        partialPattern.dfs(centerVid, dfsEdges);
        //System.out.print("dfsEdges: ");
        //System.out.println(dfsEdges);

        LinkedHashSet<Edge> edges = (LinkedHashSet<Edge>)partialPattern.getEdgesSet();
        //System.out.print("edges: ");
        //System.out.println(edges);

        edges.removeAll(dfsEdges);
        //System.out.print("nonDfsEdges: ");
        //System.out.println(edges);

        Graph tempG = new Graph();

        BigDecimal curNumMatches = BigDecimal.ONE;
        for(Edge e: dfsEdges) { //Compute case I
            if(curNumMatches.compareTo(BigDecimal.ONE) == 0) {
                curNumMatches = curNumMatches.multiply(sumOfPower.get(1));
            }
            else {
                int v1 = e.getStart();
                int v2 = e.getEnd();
                int deg = tempG.getDegree(v1);
                if(deg == 0) {
                    deg = tempG.getDegree(v2);
                }
                if(deg == 0) {
                    //throw new Exception("Both vertex has degree 0.");
                    System.out.println("Both vertex has degree 0.");
                }
                BigDecimal r1 = calGamma1(deg, degArray, sumOfPower);
                curNumMatches = curNumMatches.multiply(r1);
            }
            tempG.addEdge(e);
        }
        for(Edge e: edges) { //Compute case II
            int v1 = e.getStart();
            int v2 = e.getEnd();
            int deg1 = tempG.getDegree(v1);
            int deg2 = tempG.getDegree(v2);
            BigDecimal r2 = calGamma2(deg1, deg2, degArray, sumOfPower);
            curNumMatches = curNumMatches.multiply(r2);
            tempG.addEdge(e);
        }
        return curNumMatches;
    }

    private BigDecimal calGamma1(int d, ArrayList<Integer> degArray, HashMap<Integer, BigDecimal> sumOfPower) {
        BigDecimal sum1 = BigDecimal.ZERO;
        BigDecimal sum2 = BigDecimal.ZERO;
        if(!sumOfPower.containsKey(d)) {
            for(int deg: degArray) {
                sum1 = sum1.add(BigDecimal.valueOf(deg).pow(d));
            }
            sumOfPower.put(d, sum1);
        } else {
            sum1 = sum1.add(sumOfPower.get(d));
        }

        if(!sumOfPower.containsKey(d+1)) {
            for(int deg: degArray) {
                sum2 = sum2.add(BigDecimal.valueOf(deg).pow(d+1));
            }
            sumOfPower.put(d+1, sum2);
        } else {
            sum2 = sum2.add(sumOfPower.get(d+1));
        }

        BigDecimal ratio = sum2.divide(sum1, 32, BigDecimal.ROUND_HALF_UP);
        return ratio;
    }

    private BigDecimal calGamma2(int d1, int d2, ArrayList<Integer> degArray, HashMap<Integer, BigDecimal> sumOfPower) {
        BigDecimal ratio = calGamma1(d1, degArray, sumOfPower);
        ratio = ratio.multiply(calGamma1(d2, degArray, sumOfPower));
        ratio = ratio.divide(sumOfPower.get(1), 32, BigDecimal.ROUND_HALF_UP);
        return ratio;
    }
}
