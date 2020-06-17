package cn.edu.nju.pasalab.db.test;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import cn.edu.nju.pasalab.db.redis.ShardedRedisClusterClient;
import cn.edu.nju.pasalab.db.Utils;

import java.io.FileInputStream;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by wangzhaokang on 4/11/18.
 */
public class TestClient {

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
        Logger logger = Logger.getLogger("Test client...");
        logger.info("Start preparing data...");
        assert args.length >= 2;
        String confFile = args[0];
        String clientClassName = args[1];
        // Load test properties
        Properties conf = new Properties();
        FileInputStream inputStream = new FileInputStream(confFile);
        conf.load(inputStream);
        inputStream.close();
        logger.info("Load configurations: " + conf);
        logger.info("Test client: " + clientClassName);
        BasicKVDatabaseClient client = (BasicKVDatabaseClient) Class.forName(clientClassName).newInstance();
        client.connect(conf);

        // Prepare random data
        Set<Integer> keySet = new HashSet<>();
        ArrayList<ArrayList<Tuple<Integer, byte[]>>> data = new ArrayList<>();
        Random random = new Random(System.nanoTime());
        for (int i = 0; i < 100; i++) {
            int randomItemNum = Math.abs(random.nextInt() % 500) + 1;
            ArrayList<Tuple<Integer, byte[]>> items = new ArrayList<>(randomItemNum);
            for (int j = 0; j < randomItemNum; j++) {
                int keyInt = Math.abs(random.nextInt());
                while (keySet.contains(keyInt)) {
                    keyInt = Math.abs(random.nextInt());
                }
                keySet.add(keyInt);
                byte key[] = intToByteArray(keyInt);
                random.nextBytes(key);
                int randomValueLength = Math.abs(random.nextInt() % 50)+ 1;
                byte value[] = new byte[randomValueLength];
                random.nextBytes(value);
                items.add(new Tuple<Integer, byte[]>(keyInt, value));
            }
            data.add(items);
        }
        logger.info("Start single put & get test...");
//         clear database
        client.clearDB();
//         Check single put & get
        data.parallelStream().forEach(items -> {
            try {
                for (Tuple<Integer, byte[]> tuple : items) {
                    client.put(intToByteArray(tuple.x), tuple.y);
//                    byte[] valueGot;
//                    valueGot = client.get(intToByteArray(tuple.x));
//                    if(!Arrays.equals(tuple.y, valueGot)) System.exit(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        data.parallelStream().forEach(items -> {
            try {
                for (Tuple<Integer, byte[]> tuple : items) {
                    byte valueGot[] = client.get(intToByteArray(tuple.x));
                    if (!Arrays.equals(valueGot, tuple.y)) {
                        System.out.println("Different for key:" + tuple.x + "!");
                        System.out.println("Got:" + Arrays.toString(valueGot));
                        System.out.println("Expect:" + Arrays.toString(tuple.y));
                    }
                    assert Arrays.equals(valueGot, tuple.y);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        // Check batch put & get
        client.clearDB();
        data.parallelStream().forEach(items -> {
            try {
                byte keys[][], values[][];
                keys = new byte[items.size()][];
                values = new byte[items.size()][];
                for (int i = 0; i < items.size(); i++) {
                    keys[i] = intToByteArray(items.get(i).x);
                    values[i] = items.get(i).y;
                }
                client.putAll(keys, values);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        data.parallelStream().forEach(items -> {
            try {
                byte keys[][], values[][];
                keys = new byte[items.size()][];
                for (int i = 0; i < items.size(); i++) {
                    keys[i] = intToByteArray(items.get(i).x);
                }
                values = client.getAll(keys);
                for (int i = 0; i < items.size(); i++) {
                    assert Arrays.equals(values[i], items.get(i).y);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        logger.info("Start batch put & get test...");
        // Check batch put & get with limited batch size
        client.clearDB();
        logger.info("Start batch inserting...");
        data.parallelStream().forEach(items -> {
            try {
                byte keys[][], values[][];
                keys = new byte[items.size()][];
                values = new byte[items.size()][];
                for (int i = 0; i < items.size(); i++) {
                    keys[i] = intToByteArray(items.get(i).x);
                    values[i] = items.get(i).y;
                }
                Utils.batchInput(client, keys, values, 20);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        logger.info("Start batch fetching...");
        data.parallelStream().forEach(items -> {
            try {
                byte keys[][], values[][];
                keys = new byte[items.size()][];
                for (int i = 0; i < items.size(); i++) {
                    keys[i] = intToByteArray(items.get(i).x);
                }
                values = client.getAll(keys);
                for (int i = 0; i < items.size(); i++) {
                    assert Arrays.equals(values[i], items.get(i).y);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        logger.info("Start testing null key fetch");
        int nullKey = -1;
        byte[][] nullKeys = new byte[3][];
        nullKeys[0] = intToByteArray(-1);
        nullKeys[1] = intToByteArray(-2);
        nullKeys[2] = intToByteArray(-3);
        byte[][] results = client.getAll(nullKeys);
        for (byte[] r : results)
            logger.info("Null fetch result:" + r);

        client.close();
        logger.info("Done!");
    }

    static class Tuple<X, Y> {
        public final X x;
        public final Y y;

        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }
}
