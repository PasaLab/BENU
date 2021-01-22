package cn.edu.nju.pasa.graph.analysis.subgraph.hadoop;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.apache.hadoop.io.*;
import com.google.common.hash.HashFunction;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Store adjacency list for each vertex.
 */
public final class VertexCentricAnalysisTask implements WritableComparable<VertexCentricAnalysisTask> {
    private int vid = -1;
    private int[] adj = null;
    private short randomSalt; // Use the random salt to shuffle the order of the tasks on the reducer side

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    private int start = -1;
    private int end = -1;

    public VertexCentricAnalysisTask() {
        randomSalt = (short)System.nanoTime();
    }

    public VertexCentricAnalysisTask(int vid, int[] adj) {
        this();
        this.vid = vid;
        this.adj = adj;
        this.start = -1;
        this.end = -1;
    }
    public VertexCentricAnalysisTask(int vid, int[] adj, int start, int end) {
        this();
        this.vid = vid;
        this.adj = adj;
        this.start = start;
        this.end = end;
    }


    public int getVid() {
        return vid;
    }

    public void setVid(int vid) {
        this.vid = vid;
    }

    public int[] getAdj() {
        return adj;
    }

    public void setAdj(int[] adj) {
        this.adj = adj;
    }

    @Override
    public int hashCode() {
        byte[] rawArray = new byte[Integer.BYTES * 3];
        ByteBuffer infoBuffer = ByteBuffer.wrap(rawArray);
        infoBuffer.putInt(start); infoBuffer.putInt(end); infoBuffer.putInt(vid);
        HashCode hashCode = Hashing.md5().hashBytes(rawArray);
        return hashCode.asInt();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(vid);
        for (int i = 0; i < adj.length; i++) {
            builder.append(" " + adj[i]);
        }
        return builder.toString();
    }

    public static VertexCentricAnalysisTask buildFromString(String str) {
        String[] fields = str.split(" ");
        int vid=  Integer.parseInt(fields[0]);
        int[] adj = new int[fields.length - 1];
        for (int i = 0; i < adj.length; i++) {
            adj[i] = Integer.parseInt(fields[i + 1]);
        }
        return new VertexCentricAnalysisTask(vid, adj);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeShort(randomSalt);
        dataOutput.writeInt(vid);
        dataOutput.writeInt(start);
        dataOutput.writeInt(end);
        if (adj == null) {
            dataOutput.writeInt(-1);
        } else {
            dataOutput.writeInt(adj.length);
            for (int i = 0; i < adj.length; i++) {
                dataOutput.writeInt(adj[i]);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.randomSalt = dataInput.readShort();
        this.vid = dataInput.readInt();
        this.start = dataInput.readInt();
        this.end = dataInput.readInt();
        int adjLength = dataInput.readInt();
        if (adjLength == -1) {
            this.adj = null;
        } else {
            this.adj = new int[adjLength];
            for (int i = 0; i < adjLength; i++) {
                this.adj[i] = dataInput.readInt();
            }
        }
    }

    @Override
    public int compareTo(VertexCentricAnalysisTask task) {
        // compare the random salt first
        if (this.randomSalt > task.randomSalt) return 1;
        if (this.randomSalt < task.randomSalt) return -1;
        if (this.vid > task.vid) return 1;
        else if (this.vid < task.vid) return -1;
        else {
            if (this.start > task.start) return 1;
            else if (this.start < task.start) return -1;
            else {
                if (this.end > task.end) return 1;
                else if (this.end < task.end) return -1;
                else return 0;
            }
        }
    }

    /** A Comparator optimized for VertexCentricAnalysisTask. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(VertexCentricAnalysisTask.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            for (int i = 0; i < 12; i++) {
                if (b1[s1+i] - b2[s2+i] != 0) return b1[s1+i] - b2[s2+i];
            }
            return 0;
        }
    }

    static {                                        // register this comparator
        WritableComparator.define(VertexCentricAnalysisTask.class, new Comparator());
    }
}
