package cn.edu.nju.pasa.graph.analysis.subgraph.dynamic;

import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexTaskStats;
import cn.edu.nju.pasa.graph.util.HDFSUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TaskStatusOutputCollector {
    private String filePath;
    private FSDataOutputStream outputStream;
    public TaskStatusOutputCollector(String filePath) throws IOException {
        this.filePath = filePath;
        FileSystem hdfs = HDFSUtil.getFS();
        Path path  = new Path(filePath);
        FSDataOutputStream outputStream = hdfs.create(path, true);
        this.outputStream = outputStream;
    }
    public void offer(VertexTaskStats status) throws IOException {
        outputStream.writeChars(status.toString() + "\n");
    }
    public void cleanup() throws IOException {
        outputStream.close();
    }
}
