package cn.edu.nju.pasalab.db;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * The abstract class for a simple key-value database client.
 */
public abstract class BasicKVDatabaseClient {


    /**
     * thread-safe
     * @param key
     * @return the value. Return null if the key does not exist.
     * @throws Exception
     */
    public abstract byte[] get(byte key[]) throws Exception;
    public abstract void put(byte key[], byte value[]) throws Exception;

    /**
     * Get the values of a group of keys
     * thread-safe
     * @param keys
     * @return the values of the keys
     * @throws Exception
     */
    public byte[][] getAll(byte keys[][]) throws Exception{
        byte result[][] = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            result[i] = get(keys[i]);
        }
        return result;
    }

    /**
     * Get the values of a group of keys
     * thread-safe
     * @param keys
     * @param from the start index of keys
     * @param to the end index of keys
     * @return the corresponding values of the keys
     * @throws Exception
     */
    public byte[][] getAll(byte keys[][], int from, int to) throws Exception {
        assert from >= 0 && from < to;
        assert keys.length <= to;
        byte queryKeys[][] = new byte[to - from][];
        for (int i = from; i < to; i++) {
            queryKeys[i - from] = keys[i];
        }
        byte result[][] = getAll(queryKeys);
        return result;
    }

    /**
     * Set the key-value pairs in batch
     * thread-safe
     * @param keys
     * @param values
     * @throws Exception
     */
    public void putAll(byte keys[][], byte values[][]) throws Exception {
        assert keys.length == values.length;
        for (int i = 0; i < keys.length; i++) {
            put(keys[i], values[i]);
        }
    }

    /**
     * Set the key-value pairs in batch
     * thread-safe
     * @param keys
     * @param values
     * @param from the start index
     * @param to the end index
     * @throws Exception
     */
    public void putAll(byte keys[][], byte values[][], int from, int to) throws Exception {
        assert keys.length == values.length;
        assert from >= 0 && from < to;
        assert to <= keys.length;
        byte insertKeys[][] = new byte[to - from][];
        byte insertValues[][] = new byte[to - from][];
        for (int i = from; i < to; i++) {
            insertKeys[i - from] = keys[i];
            insertValues[i - from] = values[i];
        }
        putAll(insertKeys, insertValues);
    }

    /**
     * Close the database connections.
     */
    public abstract void close() throws Exception;

    /**
     * Connect to the underlying database
     * @param conf Database connection-related configurations
     * @throws Exception database connection exception
     */
    public abstract void connect(Properties conf) throws Exception;

    /**
     * Clear the contents in the database
     */
    public abstract void clearDB() throws Exception;

    public abstract void createDB() throws Exception;
}
