package cn.edu.nju.pasa.graph.storage;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Map;

/**
 * The abstract class for a simple graph storage.
 * Simple graph storage:
 *  1. Vertex ID: int
 *  2. Adjacency set of a vertex: A sorted array according to vertex IDs.
 *  3. Do not support properties.
 *  4. Only support put and get operations.
 */
public abstract class AbstractSimpleGraphStorage {
    /**
     * Query the adjacency set of a vertex
     * @param vid the id of the vertex
     * @return the adjacency list of the vertex
     */
    public abstract int[] get(int vid) throws Exception;

    /**
     * Set the adjacency set of a vertex
     * @param vid the id of the vertex
     * @param adj the adjacency list of the vertex
     */
    public abstract void put(int vid, int[] adj) throws Exception;

    /**
     * Query multiple vertices in a batch
     * @param vids
     * @return
     */
    public Map<Integer, int[]> get(int[] vids) throws Exception {
        Int2ObjectMap<int[]> results = new Int2ObjectOpenHashMap<>();
        for (int i = 0; i < vids.length; i++) {
            results.putIfAbsent(vids[i], get(vids[i]));
        }
        return results;
    }

    /**
     * Set multiple vertices in a batch
     * @param vid2Adj
     */
    public void put(Int2ObjectMap<int[]> vid2Adj) throws Exception {
        vid2Adj.forEach((vid, adj) -> {
            try {
                put(vid, adj);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Close underlying database connections.
     */
    public abstract void close();

    /**
     * Connect to the underlying database
     * @throws Exception database connection exception
     */
    public abstract void connect() throws Exception;

    /**
     * Clear the content in the database
     */
    public abstract void clearDB();

}