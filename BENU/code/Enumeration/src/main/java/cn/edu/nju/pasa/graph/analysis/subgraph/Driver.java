package cn.edu.nju.pasa.graph.analysis.subgraph;

import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.AdjFileMapper;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.AnalysisTaskReducer;
import cn.edu.nju.pasa.graph.analysis.subgraph.hadoop.VertexCentricAnalysisTask;
import cn.edu.nju.pasa.graph.analysis.subgraph.util.CommandLineUtils;
import cn.edu.nju.pasa.graph.storage.CacheBasedDistributedGraphStorage;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by wangzhaokang on 4/13/18.
 */
public class Driver {

    public static void main(String args[]) throws Exception {
        Logger logger = Logger.getLogger("Driver");

        Configuration configuration = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(configuration, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();
        Properties jobProp = CommandLineUtils.parseArgs(remainingArgs);
        String dataGraphPath = jobProp.getProperty(MyConf.DATA_GRAPH_PATH);
        String enumeratorClassName = jobProp.getProperty(MyConf.ENUMERATOR_CLASS);
        String outputPath = jobProp.getProperty(MyConf.OUTPUT_FILE_PATH);
        int numExecutors = Integer.parseInt(jobProp.getProperty(MyConf.NUM_EXECUTORS, MyConf.Default.NUM_EXECUTORS));
        boolean storeGraph = Boolean.parseBoolean(jobProp.getProperty(MyConf.STORE_GRAPH_TO_DB, "false"));
        // Check the status of database
        boolean needRefreshDB = checkDBStatus(dataGraphPath);
        if (needRefreshDB || storeGraph) {
            if (needRefreshDB)
                System.err.println("The data graph stored in the database is not equal to the one presented as the data graph.");
            System.err.println("Now clear the database and store the data graph in the map phase");
            try{
                CacheBasedDistributedGraphStorage.getProcessLevelStorage().clearDB();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(102);
            }
            jobProp.setProperty(MyConf.STORE_GRAPH_TO_DB, "true");
        }
        setPropToHadoopConf(configuration, jobProp);


        long t0 = System.nanoTime();
        Job job = Job.getInstance(configuration, "Subgraph enumeration-" + enumeratorClassName);
        job.setJarByClass(Driver.class);
        FileInputFormat.addInputPaths(job, dataGraphPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(AdjFileMapper.class);
        job.setMapOutputKeyClass(VertexCentricAnalysisTask.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(AnalysisTaskReducer.class);
        job.setReduceSpeculativeExecution(false);
        job.setMaxReduceAttempts(1);
        job.setNumReduceTasks(numExecutors);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean flag;
        logger.info("Submit MR job...");
        flag = job.waitForCompletion(true);
        long t1 = System.nanoTime();
        logger.info("MR job done!");
        System.out.println("MR Job elapsed time (s): " + (t1 - t0)/1e9);

        // Update the database about the graph stored in it
        if (true) {
            System.err.println("Storing database status...");
            setDBStatus(dataGraphPath);
        }

        System.exit(0);
    }

    private static void setPropToHadoopConf(Configuration conf, Properties prop) {
        for(String propKey: prop.stringPropertyNames()) {
            conf.set(propKey, prop.getProperty(propKey));
        }
    }

    /**
     * Check whether the data graph stored in the database is the one appointed in the `filePath`.
     * @param filePath
     * @return true, need to store to db; false, does not!
     */
    private static boolean checkDBStatus(String filePath) {
        System.err.println("Start checking database validation...");
        int hash =  Hashing.md5().hashString(filePath, Charsets.UTF_16).asInt();
        // we use the key -100 to store the hash code of the file path.
        try {
            int[] ret = CacheBasedDistributedGraphStorage.getProcessLevelStorage().get(-100);
            if (ret != null && ret.length > 0) {
                int code = ret[0];
                return code != hash;
            } else {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return true;
        }
    }

    private static void setDBStatus(String filePath) {
        int hash =  Hashing.md5().hashString(filePath, Charsets.UTF_16).asInt();
        int arr[] = new int[1];
        arr[0] = hash;
        try {
            CacheBasedDistributedGraphStorage.getProcessLevelStorage().put(-100, arr);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
