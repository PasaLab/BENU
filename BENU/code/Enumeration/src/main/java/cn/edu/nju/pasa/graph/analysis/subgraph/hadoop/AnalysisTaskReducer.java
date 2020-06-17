package cn.edu.nju.pasa.graph.analysis.subgraph.hadoop;

import cn.edu.nju.pasa.graph.analysis.subgraph.MyConf;
import cn.edu.nju.pasa.graph.analysis.subgraph.enumerator.EnumerateOnCenterVertexTaskInterface;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wangzhaokang on 4/13/18.
 */
public class AnalysisTaskReducer extends Reducer<VertexCentricAnalysisTask, NullWritable, Text, NullWritable> {

    private Properties jobProp, processProp;
    private EnumerateOnCenterVertexTaskInterface enumerator;
    private Thread[] workerThreads;
    private Thread resultCollectingThread;
    private ArrayBlockingQueue<VertexCentricAnalysisTask> taskQueue;
    private ArrayBlockingQueue<VertexTaskStats> resultsCollectQueue;
    private AtomicLong taskNum, resultNum;
    private Context currentContext;
    private Counter embeddingNumCounter, vertexNumCounter, dbAccessTimeCounter, computeTimeCounter, executionTimeCounter;
    private boolean allTaskSubmitted = false;
    private long t0,t1,t2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        currentContext = context;
        super.setup(context);
        t0 = System.currentTimeMillis();
        try {
            prepareEnumerator();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            throw new IOException("Can not create the enumerator!");
        }
        prepareRunningThreads();
        prepareCounters();
        System.err.println("[Reducer] Setup done!");
        t1 = System.currentTimeMillis();
    }

    @Override
    protected void reduce(VertexCentricAnalysisTask key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //System.err.println("[Mapper] Map function call. value = " + value);
        taskNum.incrementAndGet();
        taskQueue.put(new VertexCentricAnalysisTask(key.getVid(), key.getAdj(), key.getStart(), key.getEnd()));
        System.err.println("[Reducer] Submit task for vertex " + key.getVid());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        allTaskSubmitted = true;
        System.err.println("[Reducer] In the cleanup, waiting for the result collecting thread to exit...");
        resultCollectingThread.join();
        System.err.println("[Reducer] All results get!");
        enumerator.cleanup();
        t2 = System.currentTimeMillis();
        System.err.println("[Reducer] Wall-clock execution time (ms):");
        System.err.println("[Reducer]    Prepare:" + (t1 - t0));
        System.err.println("[Reducer]    Compute:" + (t2 - t1));
        System.err.println("[Reducer]    Total:" + (t2 - t0));
    }


    private void prepareEnumerator() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String enumeratorClass = currentContext.getConfiguration().get(MyConf.ENUMERATOR_CLASS);
        System.err.println("[Reducer] Load enumerator " + enumeratorClass + " ...");
        enumerator = (EnumerateOnCenterVertexTaskInterface)Class.forName(enumeratorClass).newInstance();
        // prepare properties
        jobProp = new Properties();
        for (Map.Entry<String, String> entry : currentContext.getConfiguration()) {
            jobProp.setProperty(entry.getKey(), entry.getValue());
        }
        enumerator.prepare(jobProp);
    }

    private void prepareRunningThreads() {
        // Prepare queues
        int blockingQueueSize = Integer.parseInt(jobProp.getProperty(MyConf.BLOCKING_QUEUE_SIZE, "1000"));
        System.err.println("[Mapper] Prepare queues...");
        taskQueue = new ArrayBlockingQueue<VertexCentricAnalysisTask>(blockingQueueSize);
        resultsCollectQueue = new ArrayBlockingQueue<VertexTaskStats>(blockingQueueSize);
        // Prepare threads
        System.err.println("[Mapper] Prepare threads...");
        int threadNum = Integer.parseInt(jobProp.getProperty(MyConf.NUM_WORKING_THREADS,
                MyConf.Default.NUM_THREADS_PER_MAPPER));
        workerThreads = new Thread[threadNum];
        for (int i = 0; i < workerThreads.length; i++) {
            workerThreads[i] = new Thread(new WorkingThread());
            workerThreads[i].setName("Enumeration worker thread " + i);
            workerThreads[i].setDaemon(false);
            workerThreads[i].start();
        }
        resultCollectingThread = new Thread(new ResultCollectingThread());
        resultCollectingThread.setName("Result collecting thread");
        resultCollectingThread.setDaemon(false);
        resultCollectingThread.start();

        // Prepare indicators
        taskNum = new AtomicLong(0L);
        resultNum = new AtomicLong(0L);
    }

    private void prepareCounters() {
        embeddingNumCounter = currentContext.getCounter("Result","embeddings.num");
        vertexNumCounter = currentContext.getCounter("Result","vertex.num");
        dbAccessTimeCounter = currentContext.getCounter("Result", "dbAccessTime(ns)");
        computeTimeCounter = currentContext.getCounter("Result", "computeTime(ns)");
        executionTimeCounter = currentContext.getCounter("Result", "serialExecutionTime(ns)");
    }


    private class WorkingThread implements Runnable {

        @Override
        public void run() {
            try {
                System.err.println("[Reducer] Working thread " + Thread.currentThread().getName() + " starts!");
                while (true) {
                    VertexCentricAnalysisTask task = taskQueue.take();
                    VertexTaskStats stats = enumerator.enumerate(task.getVid(), task);
                    resultsCollectQueue.put(stats);
                    System.err.println("[Reducer] task for vertex " + task.getVid() + " done!");
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private class ResultCollectingThread implements Runnable {

        boolean output;

        public ResultCollectingThread() {
            output = !jobProp.getProperty(MyConf.OUTPUT_FILE_PATH).equals("null");
        }

        @Override
        public void run() {
            System.err.println("[Mapper] Result collecting thread start!");
            while (!(resultNum.get() >= taskNum.get() && allTaskSubmitted)) {
                try {
                    if (resultNum.get() < taskNum.get()) {
                        VertexTaskStats stats = resultsCollectQueue.take();
                        if (output)
                            currentContext.write(new Text(stats.toString()), NullWritable.get());
                        resultNum.incrementAndGet();
                        executionTimeCounter.increment(stats.getExecutionTime());
                        embeddingNumCounter.increment(stats.getEnumerateResultsCount());
                        vertexNumCounter.increment(1L);
                        System.err.println("[Reducer] get a result: " + stats);
                        System.err.println("[Reducer] current task num / result num: " + taskNum.get() + "/" + resultNum.get());
                        System.err.flush();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.flush();
                    System.err.println(e.getMessage());
                    System.err.flush();
                    System.exit(41);
                }
            }
            System.err.println("[Reducer] All results get. Now exit the result collecting thread...");
        }
    }
}
