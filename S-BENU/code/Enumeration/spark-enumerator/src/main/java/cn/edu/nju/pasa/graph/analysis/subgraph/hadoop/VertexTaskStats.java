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
    public long vid;
    public int degree;
    public long twoHopNeighborEdgeNum;
    public long enumerateResultsCount;
    public long executionTime; // in nano second
    public String hostname;
    public long dbqCount; // number of executed DBQ instructions
    public long dbQueryingTime; // execution time spent on database querying (in nano second)
    public long intCount; // number of executed INT instructions
    public long enuCount = 0;

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
    public VertexTaskStats(long vid, int degree, long twoHopNeighborEdgeNum, long enumerateResultsCount,
                           long executionTime, String hostname, long dbqCount, long intCount, long dbQueryingTime, long enuCount) {
        this.vid = vid;
        this.degree = degree;
        this.twoHopNeighborEdgeNum = twoHopNeighborEdgeNum;
        this.enumerateResultsCount = enumerateResultsCount;
        this.executionTime = executionTime;
        this.hostname = hostname;
        this.dbqCount = dbqCount;
        this.intCount = intCount;
        this.dbQueryingTime = dbQueryingTime;
        this.enuCount = enuCount;
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
                + executionTime + "," + dbqCount + "," + intCount + "," + dbQueryingTime
                + "," + hostname;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }
}
