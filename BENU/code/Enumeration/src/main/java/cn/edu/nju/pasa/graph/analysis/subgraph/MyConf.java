package cn.edu.nju.pasa.graph.analysis.subgraph;

/**
 * Created by bsidb on 12/4/2017.
 */
public class MyConf {
    /** Set by users */
    public static final String DATA_GRAPH_PATH="data.graph.path";
    public static final String OUTPUT_FILE_PATH="output.path";
    public static final String QUERY_GRAPH_TYPE="query.graph.type";
    public static final String NUM_TASK_GROUPS="num.task.groups";
    public static final String NUM_EXECUTORS="num.executors";
    public static final String ENUMERATOR_CLASS="enumerator.class";
    public static final String NUM_WORKING_THREADS="num.working.threads";
    public static final String MEMORY_PER_MAPPER_MB="memory.per.mapper.mb";
    public static final String BLOCKING_QUEUE_SIZE="blocking.queue.size";
    public static final String SCHEDULER_SERVER_PORT ="scheduler.port";
    public static final String SCHEDULER_STATIC_SCHEDULING="scheduler.static.scheduling";
    public static final String ENABLE_ENUMERATE="enable.enumerate";
    public static final String STORE_GRAPH_TO_DB="store.graph.to.db";
    /** Set by the program */
    public static final String CONF_MY_JOB_CONF = "job.local.conf.string";
    public static final String SCHEDULER_SERVER_ADDRESS = "scheduler.server.address";
    public static final String SCHEDULER_SERVER_HOST = "scheduler.server.host";
    public static final String DATA_GRAPH_DEGREE_PATH = "data.graph.degree.path";
    public static final String ENABLE_LOAD_BALANCE = "enable.load.balance";
    public static final String LOAD_BALANCE_THRESHOLD = "load.balance.threshold";

    public static class Default {
        public static String NUM_THREADS_PER_MAPPER = "4";
        public static String NUM_EXECUTORS = "4";
        public static String MEMORY_PER_MAPPER_MB = "4096";
        public static String SCHEDULER_PORT="5066";
        public static String SCHEDULER_STATIC_SHCEDULING="false";
    }

}
