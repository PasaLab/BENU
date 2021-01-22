package cn.edu.nju.pasa.graph.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Some utils for HDFS operation
 */
public class HDFSUtil {
    public static FileSystem getFS() throws IOException {
        Configuration hadoopConf = new Configuration();
        return FileSystem.get(hadoopConf);
    }
    public static void delete(String file) throws IOException {
        getFS().delete(new Path(file), true);
    }

    public static List<Path> listFiles(String dir) throws IOException {
        return Arrays.stream(getFS().listStatus(new Path(dir)))
                .map(status -> status.getPath()).collect(Collectors.toList());
    }

    public static List<Path> listFiles(String dir, Boolean filter) throws IOException {
        if(filter) {
            PathFilter logFileFilter = (Path path) -> path.getName().startsWith("part");
            return Arrays.stream(getFS().listStatus(new Path(dir), logFileFilter))
                    .map(status -> status.getPath()).collect(Collectors.toList());
        } else {
            return listFiles(dir);
        }
    }

}