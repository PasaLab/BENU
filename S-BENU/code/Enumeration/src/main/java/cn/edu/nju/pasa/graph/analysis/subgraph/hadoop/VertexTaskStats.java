package cn.edu.nju.pasa.graph.analysis.subgraph.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Scanner;

/**
 * Store statistics for a vertex task
 * Created by wangzhaokang on 11/6/17.
 */
public final class VertexTaskStats implements Serializable {
    private long vid;
    private int degree;
    private long twoHopNeighborEdgeNum;
    private long enumerateResultsCount;
    private long executionTime; // in nano second
    private String hostname;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }


    public VertexTaskStats() {
    }

    public VertexTaskStats(long vid, int degree, long twoHopNeighborEdgeNum, long enumerateResultsCount,
                           long executionTime, String hostname) {
        this.vid = vid;
        this.degree = degree;
        this.twoHopNeighborEdgeNum = twoHopNeighborEdgeNum;
        this.enumerateResultsCount = enumerateResultsCount;
        this.executionTime = executionTime;
        this.hostname = hostname;
    }

    public long getVid() {
        return vid;
    }

    public void setVid(long vid) {
        this.vid = vid;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public long getTwoHopNeighborEdgeNum() {
        return twoHopNeighborEdgeNum;
    }

    public void setTwoHopNeighborEdgeNum(long twoHopNeighborEdgeNum) {
        this.twoHopNeighborEdgeNum = twoHopNeighborEdgeNum;
    }

    public long getEnumerateResultsCount() {
        return enumerateResultsCount;
    }

    public void setEnumerateResultsCount(long enumerateResultsCount) {
        this.enumerateResultsCount = enumerateResultsCount;
    }

    @Override
    public String toString() {
        return vid + "," + degree + "," + twoHopNeighborEdgeNum + "," + enumerateResultsCount + ","
                + executionTime + "," + hostname;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }
}
