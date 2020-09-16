package com.alibaba.datax.core.statistics.communication.redis.util;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient {

    private JedisPool pool;
    private RedisConfig redisConfig;
    private static RedisClient redisClient;

    private RedisClient(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public static synchronized RedisClient newInstance(Configuration core){
        if (redisClient == null){
            RedisConfig redisConfig = new RedisConfig(core);
            redisClient = new RedisClient(redisConfig);
            if (redisClient.pool == null) {
                String ip = redisConfig.getHost();
                int port = redisConfig.getPort();
                String password = redisConfig.getPassword();
                JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                jedisPoolConfig.setMaxTotal(redisConfig.getPool_maxTotal());
                jedisPoolConfig.setMaxIdle(redisConfig.getPool_maxIdle());
                jedisPoolConfig.setMaxWaitMillis(redisConfig.getPool_maxWaitMills());
                jedisPoolConfig.setTestOnBorrow(redisConfig.getPool_testOnBorrow());
                if (StringUtils.isNotBlank(password)) {
                    // redis 设置了密码
                    redisClient.pool = new JedisPool(jedisPoolConfig, ip, port, 10000, password);
                } else {
                    // redis 未设置密码
                    redisClient.pool = new JedisPool(jedisPoolConfig, ip, port, 10000);
                }
            }
        }
        return redisClient;
    }

    public Jedis getJedis() {
        if (redisClient == null){
            throw new IllegalStateException("Redis Client is not initial");
        }
        if (redisClient.pool == null){
            throw new IllegalStateException("Redis Connection pool initialization may be fail");
        }
        return redisClient.pool.getResource();
    }
}
