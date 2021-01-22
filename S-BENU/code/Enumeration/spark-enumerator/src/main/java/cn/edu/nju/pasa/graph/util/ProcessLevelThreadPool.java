package cn.edu.nju.pasa.graph.util;

import java.util.concurrent.ForkJoinPool;

/** A process-level thread pool used in Spark executor */
public class ProcessLevelThreadPool {
    private static volatile ForkJoinPool pool;
    public static ForkJoinPool pool(int numThreads) {
        if (pool == null) {
            synchronized (ProcessLevelThreadPool.class) {
                if (pool == null) {
                    pool = new ForkJoinPool(numThreads);
                    System.out.println("Create thread level forkjoinpool with " + numThreads + " threads.");
                }
            }
        }
        return pool;
    }
}
