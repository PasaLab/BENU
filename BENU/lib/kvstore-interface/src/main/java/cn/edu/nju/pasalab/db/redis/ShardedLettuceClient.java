package cn.edu.nju.pasalab.db.redis;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;

import com.lambdaworks.redis.LettuceFutures;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.codec.RedisCodec;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by wzk on 18-4-28.
 */
public final class ShardedLettuceClient extends BasicKVDatabaseClient {

    public static final String CONF_POOL_SIZE = "redis.connection.pool.size";
    public static final String DEFAULT_POOL_SIZE = "2";
    public static final String CONF_DB_ID = "redis.db.index"; // integer
    public static final String DEFAULT_DB_ID = "0";
    public static final String CONF_HOSTS_LIST = "redis.hosts.list"; // separated by comma
    public static final String DEFAULT_HOSTS_LIST = "localhost";
    public static final String CONF_REDIS_PORT = "redis.port";
    public static final String DEFAULT_REDIS_PORT = "6379";
    public static final String CONF_TIME_OUT = "redis.timeout.in.sec"; //  in second
    public static final String DEFAULT_TIME_OUT = "36000";


    String[] hosts;
    private int databaseID;
    private int poolSize = 4;
    private int redisPort;
    private int redisTimeout;
    private Logger logger = Logger.getLogger(ShardedLettuceClient.class.getName());
    private int currentClientIndex = 0;
    private StatefulRedisConnection<byte[], byte[]>[][] connections;
    private RedisAsyncCommands<byte[], byte[]>[][] asyncCommands;
    private RedisCodec<byte[], byte[]> codec = ByteArrayCodec.INSTANCE;

    /**
     * thread-safe
     *
     * @param key
     * @return the value. Return null if the key does not exist.
     * @throws Exception
     */
    @Override
    public byte[] get(byte[] key) throws Exception {
        assert connections != null;
        assert asyncCommands != null;
        int ci = getNextClientIndex();
        int serverID = fromKeyToServerID(key);
        RedisFuture<byte[]> ret = asyncCommands[ci][serverID].get(key);
        try {
            return ret.get(1L, TimeUnit.DAYS);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Cannot get value for key: " + Arrays.toString(key), e);
            return null;
        }
    }

    @Override
    public byte[][] getAll(byte keys[][]) throws Exception{
        assert connections != null;
        assert asyncCommands != null;
        int ci = getNextClientIndex();
        RedisFuture<byte[]>[] futures = new RedisFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            byte[] key = keys[i];
            int serverID = fromKeyToServerID(key);
            futures[i] = asyncCommands[ci][serverID].get(key);
        }
        try {
            LettuceFutures.awaitAll(1, TimeUnit.DAYS, futures);
            byte[][] results = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                results[i] = futures[i].get();
            }
            return results;
        } catch (Exception e) {
            logger.log(Level.WARNING, "Fail to get the values from Redis database", e);
            return null;
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        assert connections != null;
        assert asyncCommands != null;
        int ci = getNextClientIndex();
        assert asyncCommands[ci] != null;
        int serverID = fromKeyToServerID(key);
        RedisFuture<String> ret = asyncCommands[ci][serverID].set(key, value);
        try {
            ret.await(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "Cannot set value for key: " + Arrays.toString(key), e);
        }
    }

    @Override
    public void putAll(byte keys[][], byte values[][]) throws Exception {
        assert keys.length == values.length;
        assert connections != null;
        assert asyncCommands != null;
        int ci = getNextClientIndex();
        assert asyncCommands[ci] != null;
        List<RedisFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            byte[] key = keys[i];
            int serverID = fromKeyToServerID(key);
            futures.add(asyncCommands[ci][serverID].set(key, values[i]));
        }
        LettuceFutures.awaitAll(1, TimeUnit.DAYS, futures.toArray(new RedisFuture[futures.size()]));
    }

    /**
     * Close the database connections.
     */
    @Override
    public void close() throws Exception {
        if (connections != null) {
            for (int ci = 0; ci < poolSize; ci++) {
                for (int i = 0; i < hosts.length; i++) {
                    connections[ci][i].close();
                }
            }
        }
    }

    private void loadConfiguration(Properties conf) throws ParseException {
        logger.info("Get configuration: " + conf);
        // hosts list
        String hostsString = conf.getProperty(CONF_HOSTS_LIST, DEFAULT_HOSTS_LIST);
        this.hosts = hostsString.split(",");
        // database id
        String databaseIDString = conf.getProperty(CONF_DB_ID, DEFAULT_DB_ID);
        this.databaseID = Integer.parseInt(databaseIDString);
        // pool size
        String poolSizeString = conf.getProperty(CONF_POOL_SIZE, DEFAULT_POOL_SIZE);
        this.poolSize = Integer.parseInt(poolSizeString);
        // redis port
        String portString = conf.getProperty(CONF_REDIS_PORT, DEFAULT_REDIS_PORT);
        this.redisPort = Integer.parseInt(portString);
        // timeout
        String timeoutString = conf.getProperty(CONF_TIME_OUT, DEFAULT_TIME_OUT);
        this.redisTimeout = Integer.parseInt(timeoutString);
    }

    /**
     * Connect to the underlying database
     *
     * @param conf Database connection-related configurations
     * @throws Exception database connection exception
     */
    @Override
    public void connect(Properties conf) throws Exception {
        loadConfiguration(conf);
        connections = new StatefulRedisConnection[poolSize][hosts.length];
        asyncCommands = new RedisAsyncCommands[poolSize][hosts.length];
        for (int cIndex = 0; cIndex < poolSize; cIndex++) {
            for (int i = 0; i < hosts.length; i++) {
                String redisURI = String.format("redis://%s:%d/%d?timeout=%ds",
                                                hosts[i], redisPort, databaseID, redisTimeout);
                RedisClient redisClient = RedisClient.create(redisURI);
                connections[cIndex][i] = redisClient.connect(codec);
                asyncCommands[cIndex][i] = connections[cIndex][i].async();
            }
        }
        logger.info("Init redis client with " + hosts.length + " servers done!");
    }

    /**
     * Clear the contents in the database
     */
    @Override
    public void clearDB() throws Exception {
        assert connections != null;
        assert asyncCommands != null;
        List<RedisFuture<String>> futures = Arrays.stream(connections[0]).parallel()
                .map(con -> con.async().flushdb())
                .collect(Collectors.toList());
        LettuceFutures.awaitAll(1, TimeUnit.DAYS, futures.toArray(new RedisFuture[futures.size()]));
    }

    @Override
    public void createDB() throws Exception {

    }

    private int fromKeyToServerID(byte[] key) {
        return (int) Math.abs(Arrays.hashCode(key)) % hosts.length;
    }

    private int getNextClientIndex() {
        int index = currentClientIndex;
        currentClientIndex = (currentClientIndex + 1) % poolSize;
        return index;
    }
}
