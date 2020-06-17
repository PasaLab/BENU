package cn.edu.nju.pasa.graph.analysis.subgraph.hadoop;

import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import cn.edu.nju.pasa.graph.storage.AbstractSimpleGraphStorage;
import cn.edu.nju.pasa.graph.storage.CacheBasedDistributedGraphStorage;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by wangzhaokang on 4/13/18.
 */
public class AdjFileMapper extends Mapper<LongWritable, Text, VertexCentricAnalysisTask, NullWritable> {
    private IntWritable vidKey = new IntWritable();
    private ArrayList<VertexCentricAnalysisTask> tasksList = new ArrayList<>();
    private int batchSize = 10;
    private AbstractSimpleGraphStorage graphStorage;
    private boolean enableLoadBalance = false;
    private int loadBalanceThreshold = 0;
    private String enumeratorClass = "";
    private boolean storeGraphToDB = false;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        batchSize = context.getConfiguration().getInt(MyConf.BLOCKING_QUEUE_SIZE, 10) * 100;
        enableLoadBalance = context.getConfiguration().getBoolean(MyConf.ENABLE_LOAD_BALANCE, false);
        loadBalanceThreshold = context.getConfiguration().getInt(MyConf.LOAD_BALANCE_THRESHOLD, 0);
        enumeratorClass = context.getConfiguration().get(MyConf.ENUMERATOR_CLASS);
        storeGraphToDB = context.getConfiguration().getBoolean(MyConf.STORE_GRAPH_TO_DB, false);
        try {
            graphStorage = CacheBasedDistributedGraphStorage.getProcessLevelStorage();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.toString());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String fields[] = line.split(" ");
        int vid = Integer.parseInt(fields[0]);
        int adj[] = new int[fields.length - 1];
        for (int i = 1; i < fields.length; i++) {
            adj[i-1] = Integer.parseInt(fields[i]);
        }
        Arrays.sort(adj);
        VertexCentricAnalysisTask task0 = new VertexCentricAnalysisTask(vid, adj, 0, adj.length-1);
        if (storeGraphToDB) tasksList.add(task0);
        //vidKey.set(vid);
        int threshold = loadBalanceThreshold;
        int taskNum = (adj.length - 1) / threshold + 1;
        if(enableLoadBalance && taskNum > 1) {
            int balanceAdj[] = new int[adj.length];
            int avg = adj.length / taskNum;
            int reminder = adj.length % taskNum;
            //reorder adj
            for(int i = 0; i < adj.length; i++) {
                int taskId = i % taskNum;
                int m = taskId * avg;
                int n = i / taskNum;
                int k =  Math.min(taskId, reminder);
                balanceAdj[m+n+k] = adj[i];
            }
            int start = -1;
            int end = -1;
            //partition balanceAdj
            for (int i = 0; i < taskNum; i++) {
                int k = (i + 1) <= reminder ? 1 : 0;
                start = end + 1;
                end = start + avg - 1 + k;
                VertexCentricAnalysisTask task = new VertexCentricAnalysisTask(vid, balanceAdj, start, end);
                context.write(task, NullWritable.get());
            }

        }
        else {
            //VertexCentricAnalysisTask task = new VertexCentricAnalysisTask(vid, adj, 0, adj.length-1);
            //tasksList.add(task);
            context.write(task0, NullWritable.get());
        }
        if (tasksList.size() >= batchSize) {
            if (storeGraphToDB)
                writeTasksToDB();
            else
                tasksList.clear();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (tasksList.size() > 0) writeTasksToDB();
    }

    private void writeTasksToDB() throws IOException {
        Int2ObjectMap<int[]> pairs = new Int2ObjectOpenHashMap<>();
        for (VertexCentricAnalysisTask task : tasksList) {
            pairs.put(task.getVid(), task.getAdj());
        }
        try {
            graphStorage.put(pairs);
            tasksList.clear();
            /*
            //check
            Map<Integer, int[]> getResults = graphStorage.get(pairs.keySet().toIntArray());
            int[] keys = pairs.keySet().toIntArray();
            for (int i = 0; i < keys.length; i++) {
                int vid = keys[i];
                int adj[] = pairs.get(vid);
                boolean eq = Arrays.equals(adj, getResults.get(vid));
                if (!eq) {
                    throw new IOException("Get different results! A: " + Arrays.toString(adj)
                    + "| B: " + Arrays.toString(getResults.get(vid)));
                }
            }
            */
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }
}
