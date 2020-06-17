package cn.edu.nju.pasalab.db.test;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import cn.edu.nju.pasalab.db.cache.CachedClient;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by wzk on 18-5-11.
 */
public class TestCachedClient {
    //byte 数组与 int 的相互转换
    public static int byteArrayToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }


    public static void main(String args[]) throws Exception {
        Logger logger = Logger.getLogger("Test cached client...");
        int payloadSize = Integer.parseInt(args[0]);
        int keyValuePairNum = Integer.parseInt(args[1]);
        int pauseInMillisecond = Integer.parseInt(args[2]);
        String confFile = args[3];
        // Load test properties
        Properties conf = new Properties();
        FileInputStream inputStream = new FileInputStream(confFile);
        conf.load(inputStream);
        inputStream.close();
        logger.info("Load configurations: " + conf);
        BasicKVDatabaseClient client = new CachedClient();
        client.connect(conf);
        logger.info("Clear database...");
        client.clearDB();


        logger.info("Start storing data...");
        storeKeyValuePairsToDB(client, payloadSize, keyValuePairNum);
        System.gc();

        logger.info("Start fetching data...");
        logger.info("The total execution time is at least " + (keyValuePairNum * pauseInMillisecond / 1000 + 1) + " seconds");
        logger.info("Please look carefully at the memory usage");
        fetchWithPause(client, keyValuePairNum, pauseInMillisecond);

        logger.info("Clear database...");
        client.clearDB();
        client.close();
        System.exit(0);
    }

    private static void storeKeyValuePairsToDB(BasicKVDatabaseClient client, int payloadSize, int pairNum) throws Exception {
        byte[] payLoad = new byte[payloadSize];
        byte[][] values = new byte[pairNum][];
        byte[][] keys = new byte[pairNum][];
        for (int i = 0; i < pairNum; i++) {
            keys[i] = intToByteArray(i);
            values[i] = payLoad;
        }
        client.putAll(keys, values);
        System.gc();
    }

    private static void fetchWithPause(BasicKVDatabaseClient client, int pairNum, int pauseInMillisecond) throws Exception {
        for (int i = 0; i < pairNum; i++) {
            System.gc();
            byte[] key = intToByteArray(i);
            byte[] value;
            value = client.get(key);
            Thread.sleep(pauseInMillisecond);
        }
    }
}
