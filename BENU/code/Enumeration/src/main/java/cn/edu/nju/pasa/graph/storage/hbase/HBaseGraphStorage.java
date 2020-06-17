package cn.edu.nju.pasa.graph.storage.hbase;

import cn.edu.nju.pasa.graph.storage.AbstractSimpleGraphStorage;
import cn.edu.nju.pasalab.conf.ProcessLevelConf;
import cn.edu.nju.pasalab.db.hbase.HBaseClient;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Map;
import java.util.Properties;

final public class HBaseGraphStorage extends AbstractSimpleGraphStorage {

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

    @Override
    public int[] get(int vid) throws Exception {
        HBaseClient hbaseClient = getClient(vid);
        return byteArrayToIntArray(hbaseClient.get(intToBytes(vid)));
    }

    @Override
    public void put(int vid, int[] adj) throws Exception {
        HBaseClient hbaseClient = getClient(vid);
        hbaseClient.put(intToBytes(vid), intArrayToBytes(adj));
    }

    @Override
    public Map<Integer, int[]> get(int[] vids) throws Exception {
        HBaseClient hbaseClient = getClient(vids[0]);
        byte keys[][] = new byte[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            keys[i] = intToBytes(vids[i]);
        }
        byte values[][] = hbaseClient.getAll(keys);
        Int2ObjectMap<int[]> results = new Int2ObjectOpenHashMap<>();
        for (int i = 0; i < vids.length; i++) {
            int adj[] = byteArrayToIntArray(values[i]);
            results.put(vids[i], adj);
        }
        return results;
    }

    @Override
    public void put(Int2ObjectMap<int[]> vid2Adj) throws Exception {
        int vids[] = vid2Adj.keySet().toIntArray();
        HBaseClient hbaseClient = getClient(vids[0]);
        byte keys[][] = new byte[vids.length][];
        byte values[][] = new byte[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            keys[i] = intToBytes(vids[i]);
            int adj[] = vid2Adj.get(vids[i]);
            values[i] = intArrayToBytes(adj);
        }
        hbaseClient.putAll(keys, values);
    }

    @Override
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

    @Override
    public void connect() throws Exception {
        int numClients = Integer.parseInt(pasaProperties.getProperty("num.local.hbase.clients", "4"));
        hbaseClients = new HBaseClient[numClients];
        for (int i = 0; i < numClients; i++) {
            hbaseClients[i] = new HBaseClient();
            hbaseClients[i].connect(pasaProperties);
        }
    }

    @Override
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
