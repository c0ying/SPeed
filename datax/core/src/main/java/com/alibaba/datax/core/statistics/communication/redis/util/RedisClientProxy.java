package com.alibaba.datax.core.statistics.communication.redis.util;

import com.alibaba.datax.common.util.Configuration;
import redis.clients.jedis.Jedis;

public class RedisClientProxy {

    private RedisClient redisClient;

    public RedisClientProxy(Configuration core){
        this.redisClient = RedisClient.newInstance(core);
    }

    public RedisClientProxy(RedisClient redisClient){
        this.redisClient = redisClient;
    }

    public <T> T proxy(RedisOperation redisOperation, Class<T> returnType){
        Jedis jedis = null;
        try {
            jedis = redisClient.getJedis();
           return (T) redisOperation.operation(jedis);
        } catch (Exception e) {
            throw e;
        } finally {
            if(jedis != null) jedis.close();
        }
    }

    @FunctionalInterface
    public interface RedisOperation {
        Object operation(Jedis jedis);
    }
}
