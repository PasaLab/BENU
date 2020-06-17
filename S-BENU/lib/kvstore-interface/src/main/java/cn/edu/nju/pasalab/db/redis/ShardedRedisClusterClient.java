package cn.edu.nju.pasalab.db.redis;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import redis.clients.jedis.*;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by wangzhaokang on 4/11/18.
 */
public final class ShardedRedisClusterClient extends BasicKVDatabaseClient {

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


    private List<String> hosts;
    private int databaseID;
    private int poolSize = 4;
    private int redisPort;
    private int redisTimeout;
    private ShardedJedisPool pool;
    private Logger logger = Logger.getLogger(ShardedRedisClusterClient.class.getName());


    @Override
    public byte[] get(byte[] key) throws Exception {
        byte[] value = null;
        try(ShardedJedis jedis = pool.getResource()) {
            value = jedis.get(key);
        }
        return value;
    }

    @Override
    public byte[][] getAll(byte keys[][]) throws Exception{
        byte results[][] = new byte[keys.length][];
        try(ShardedJedis jedis = pool.getResource()){
            ShardedJedisPipeline pipeline = jedis.pipelined();
            Response<byte[]> responses[] = new Response[keys.length];
            for (int i = 0; i < keys.length; i++) {
                responses[i] = pipeline.get(keys[i]);
            }
            pipeline.sync();
            for (int i = 0; i < keys.length; i++) {
                results[i] = responses[i].get();
            }
        }
        return results;
    }


    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        try(ShardedJedis jedis = pool.getResource()) {
            jedis.set(key, value);
        }
    }

    @Override
    public void putAll(byte keys[][], byte values[][]) throws Exception {
        assert keys.length == values.length;
        try(ShardedJedis jedis = pool.getResource()){
            ShardedJedisPipeline pipeline = jedis.pipelined();
            for (int i = 0; i < keys.length; i++) {
                pipeline.set(keys[i], values[i]);
            }
            pipeline.sync();
        }
    }

    @Override
    public void close() throws Exception{
        logger.info("Start closing connection pool...");
        pool.close();
        this.pool = null;
        logger.info("Connection pool closed.");
    }

    private void loadConfiguration(Properties conf) throws ParseException {
        logger.info("Get configuration: " + conf);
        // hosts list
        String hostsString = conf.getProperty(CONF_HOSTS_LIST, DEFAULT_HOSTS_LIST);
        this.hosts = Arrays.asList(hostsString.split(","));
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

    @Override
    public void connect(Properties configuration) throws Exception {
        logger.info("Start loading configurations...");
        loadConfiguration(configuration);
        logger.info("Start connecting...");
        // Prepare Jedis shards
        List<JedisShardInfo> shards =
            hosts.stream().map(host -> {
                String hostURI = String.format("redis://%s:%d/%d", host, redisPort, databaseID);
                JedisShardInfo si = new JedisShardInfo(hostURI);
                si.setPassword(null);
                si.setConnectionTimeout(redisTimeout);
                return si;
            }).collect(Collectors.toList());
        logger.info("Shards:" + shards);
        // Prepare the jedis pool
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(poolSize);
        this.pool = new ShardedJedisPool(jedisPoolConfig, shards);
        logger.info("Connection established.");
    }

    @Override
    public void clearDB() throws Exception {
       List<CompletableFuture> futures = hosts.stream().map(host -> {
           return CompletableFuture.runAsync(() -> {
               Jedis jedis = new Jedis(host, redisPort, redisTimeout);
               jedis.select(databaseID);
               jedis.flushDB();
               jedis.close();
           });
       }).collect(Collectors.toList());
       CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
    }

    @Override
    public void createDB() throws Exception {

    }
}
