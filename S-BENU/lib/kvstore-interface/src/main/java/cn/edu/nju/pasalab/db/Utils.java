package cn.edu.nju.pasalab.db;

/**
 * Created by wangzhaokang on 4/12/18.
 */
public class Utils {
    /**
     * Input a lot of key-value pairs via several batches
     * @param dbClient
     * @param keys
     * @param values
     * @param batchSize
     */
    public static void batchInput(BasicKVDatabaseClient dbClient, byte keys[][], byte values[][], int batchSize) throws Exception {
        for (int i = 0; i < keys.length;) {
            int to = (i + batchSize < keys.length) ? (i + batchSize) : keys.length;
            dbClient.putAll(keys, values, i, to);
            i = to;
        }
    }
}
