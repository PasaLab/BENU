package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.util.HDFSUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OutputCollector implements MatchesCollectorInterface {
    private long deltaCount;
    private long counteractCount;
    private List<int[]> matches;
    private String outputPath;

    public OutputCollector() {
        this.deltaCount = 0;
        this.counteractCount = 0;
        this.matches = new ArrayList<>();
    }

    public OutputCollector(String outputPath) {
        this.deltaCount = 0;
        this.counteractCount = 0;
        this.matches = new ArrayList<>();
        this.outputPath = outputPath;
    }

    @Override
    public void offer(int[] match, int op) {
        synchronized (this) {
            deltaCount++;
            counteractCount += op;
            matches.add(match);
        }
    }

    @Override
    public long getDeltaCount() {
        return deltaCount;
    }

    @Override
    public long getCounteractCount() {
        return counteractCount;
    }

    @Override
    public void cleanup() {
        try {
            writeMatchToHDFS();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(201);
        }
    }

    public void writeMatchToHDFS() throws IOException {
        FileSystem hdfs = HDFSUtil.getFS();
        Path path = new Path(outputPath);
        FSDataOutputStream outputStream = hdfs.create(path, false);
        for(int[] match : matches) {
            //System.err.println(Arrays.toString(match));
            outputStream.writeBytes(Arrays.toString(match));
            outputStream.writeBytes("\n");
        }
        outputStream.flush();
        outputStream.close();
        matches.clear();
    }
}
