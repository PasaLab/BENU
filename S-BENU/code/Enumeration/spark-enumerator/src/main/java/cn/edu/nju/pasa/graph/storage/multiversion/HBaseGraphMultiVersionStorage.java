package cn.edu.nju.pasa.graph.storage.multiversion;

import cn.edu.nju.pasalab.conf.ProcessLevelConf;
import cn.edu.nju.pasalab.db.hbase.HBaseClient;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Arrays;
import java.util.Properties;

/**
 * A distributed multiversion graph storage for old/delta forward/reverse adjs.
 * key: vid, value: oldRevOffset, deltaForOffset, deltaRevOffset, oldForAdj, oldRevAdj, deltaForAdj, deltaRevAdj
 * Create by huweiwei on 04/28/2019
 */
public class HBaseGraphMultiVersionStorage {
    Properties pasaProperties = ProcessLevelConf.getPasaConf();
    private HBaseClient hbaseClients[];

    private byte[] intToBytes(int x) {
        byte[] byteArray = new byte[4];
        byteArray[0] = (byte)x;
        byteArray[1] = (byte)(x >> 8);
        byteArray[2] = (byte)(x >> 16);
        byteArray[3] = (byte)(x >> 24);
        return byteArray;
    }

    private byte[] intArrayToBytes(int[] arr) {
        byte[] byteArray = new byte[arr.length * 4];
        for (int i = 0; i < arr.length; i++) {
            int offset = 4 * i;
            int x = arr[i];
            byteArray[0 + offset] = (byte)x;
            byteArray[1 + offset] = (byte)(x >> 8);
            byteArray[2 + offset] = (byte)(x >> 16);
            byteArray[3 + offset] = (byte)(x >> 24);
        }
        return byteArray;
    }

    /**
     * store the int adjs to a byte array: arr2Offset, ... , arr4Offset, arr1Elements, ... , arr4Elements
     */
    private byte[] intAdjsToBytes(int[] arr1, int[] arr2, int[] arr3, int[] arr4) {
        byte[] byteArray = new byte[(3 + arr1.length + arr2.length + arr3.length + arr4.length) * 4];

        int[] arrLength = new int[]{arr1.length, arr2.length, arr3.length};
        int x = 3;
        for (int i = 0; i < 3; i++) { // store the arr start offset for arr2 ... arr4
            int offset = 4 * i;
            x += arrLength[i];
            byteArray[0 + offset] = (byte)x;
            byteArray[1 + offset] = (byte)(x >> 8);
            byteArray[2 + offset] = (byte)(x >> 16);
            byteArray[3 + offset] = (byte)(x >> 24);
        }

        addIntArrayToBytes(arr1, byteArray, 3);
        addIntArrayToBytes(arr2, byteArray, 3 + arr1.length);
        addIntArrayToBytes(arr3, byteArray, 3 + arr1.length + arr2.length);
        addIntArrayToBytes(arr4, byteArray, 3 + arr1.length + arr2.length + arr3.length);

        return byteArray;
    }

    private void addIntArrayToBytes(int[] intArray, byte[] byteArray, int byteArrStartPos) {
        for (int i = 0; i < intArray.length; i++) {
            int offset = 4 * (i + byteArrStartPos);
            int x = intArray[i];
            byteArray[0 + offset] = (byte)x;
            byteArray[1 + offset] = (byte)(x >> 8);
            byteArray[2 + offset] = (byte)(x >> 16);
            byteArray[3 + offset] = (byte)(x >> 24);
        }
    }

    private int[] byteArrayToIntArray(byte[] arr) {
        int x[] = new int[arr.length / 4];
        for (int i = 0; i < arr.length; i += 4) {
            x[i/4] = (0xFF & arr[0 + i]) | (0xFF & arr[1 + i]) << 8 | (0xFF & arr[2 + i]) << 16
                    | (0xFF & arr[3 + i]) << 24;
        }
        return x;
    }

    private HBaseClient getClient(int vid) {
        int clientID = Math.abs(vid) % hbaseClients.length;
        return hbaseClients[clientID];
    }

    public int[][] get(int vid) throws Exception {
        HBaseClient hbaseClient = getClient(vid);
        byte[] values = hbaseClient.get(intToBytes(vid));
        int[][] result = new int[4][];
        if(values == null) {
            // the key doesn't exist in HBase
            result[0] = new int[0];
            result[1] = new int[0];
            result[2] = new int[0];
            result[3] = new int[0];
        } else {
            int[] adjs = byteArrayToIntArray(values);
            result[0] = Arrays.copyOfRange(adjs, 3, adjs[0]); // oldForwardAdj
            result[1] = Arrays.copyOfRange(adjs, adjs[0], adjs[1]); // oldReverseAdj
            result[2] = Arrays.copyOfRange(adjs, adjs[1], adjs[2]); // deltaForwardAdj
            result[3] = Arrays.copyOfRange(adjs, adjs[2], adjs.length); // deltaReverseAdj
        }

        return result;
    }

    public void put(int vid,
                    int[] oldForwardAdj,
                    int[] oldReverseAdj,
                    int[] deltaForwardAdj,
                    int[] deltaReverseAdj) throws Exception {
        HBaseClient hbaseClient = getClient(vid);
        byte[] serializedByteArray  = intAdjsToBytes(oldForwardAdj, oldReverseAdj, deltaForwardAdj, deltaReverseAdj);
        hbaseClient.put(intToBytes(vid), serializedByteArray);
    }

    public Int2ObjectMap<int[][]> get(int[] vids) throws Exception {
        HBaseClient hbaseClient = getClient(vids[0]);
        byte keys[][] = new byte[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            keys[i] = intToBytes(vids[i]);
        }
        byte values[][] = hbaseClient.getAll(keys);
        Int2ObjectMap<int[][]> results = new Int2ObjectOpenHashMap<>();
        for (int i = 0; i < vids.length; i++) {
            int[][] result = new int[4][];
            if(values[i] == null) {
                // the key doesn't exist in HBase
                result[0] = new int[0];
                result[1] = new int[0];
                result[2] = new int[0];
                result[3] = new int[0];
            } else {
                int[] adjs = byteArrayToIntArray(values[i]);
                result[0] = Arrays.copyOfRange(adjs, 3, adjs[0]); // oldForwardAdj
                result[1] = Arrays.copyOfRange(adjs, adjs[0], adjs[1]); // oldReverseAdj
                result[2] = Arrays.copyOfRange(adjs, adjs[1], adjs[2]); // deltaForwardAdj
                result[3] = Arrays.copyOfRange(adjs, adjs[2], adjs.length); // deltaReverseAdj
            }
            results.put(vids[i], result);
        }
        return results;
    }

    public void put(Int2ObjectMap<int[][]> vid2Adjs) throws Exception {
        int vids[] = vid2Adjs.keySet().toIntArray();
        HBaseClient hbaseClient = getClient(vids[0]);
        byte keys[][] = new byte[vids.length][];
        byte values[][] = new byte[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            keys[i] = intToBytes(vids[i]);
            int[][] adjs = vid2Adjs.get(vids[i]);
            values[i] = intAdjsToBytes(adjs[0], adjs[1], adjs[2], adjs[3]);
        }
        hbaseClient.putAll(keys, values);
    }

    public void close() {
        try {
            for (HBaseClient hbaseClient: hbaseClients) {
                hbaseClient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.flush();
            System.exit(101);
        }

    }

    public void connect() throws Exception {
        int numClients = Integer.parseInt(pasaProperties.getProperty("num.local.hbase.clients", "4"));
        hbaseClients = new HBaseClient[numClients];
        for (int i = 0; i < numClients; i++) {
            hbaseClients[i] = new HBaseClient();
            hbaseClients[i].connect(pasaProperties);
        }
    }

    public void clearDB() {
        try {
            hbaseClients[0].clearDB();
        } catch (Exception e) {
            try {
                hbaseClients[0].createDB();
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(102);
            }
        }
    }
}
