import cn.edu.nju.pasa.graph.storage.redis.LongLongArrayCodec;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.*;
import com.lambdaworks.redis.api.sync.*;

public class LettuceTest {
    private static long payload[] = new long[10000];
    {
        for (int i = 0; i < payload.length; i++) {
            payload[i] = i;
        }
    }

    public static void main(String args[]) throws Exception {
        RedisClient client = RedisClient.create("redis://localhost:6379/0?timeout=1d");
        StatefulRedisConnection<Long, long[]> connection = client.connect(new LongLongArrayCodec());
        RedisCommands<Long, long[]> commands = connection.sync();
        commands.flushall();
        int loopTimes = 100000;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < loopTimes; i++) {
            commands.set((long) (i % 10), payload);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Throughput: " + (loopTimes * (long)payload.length * 8 / (t2 - t1) * 1000 / Math.pow(2.0, 20)) + " MB/s.");
        connection.close();
    }

    private static String longArrayToString(long arr[]) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("[");
        for (int i = 0; i < arr.length; i++) {
            buffer.append(arr[i]);
            if (i != arr.length - 1)
                buffer.append(',');
        }
        buffer.append("]");
        return buffer.toString();
    }
}