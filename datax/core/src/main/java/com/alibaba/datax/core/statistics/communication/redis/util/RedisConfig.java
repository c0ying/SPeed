package com.alibaba.datax.core.statistics.communication.redis.util;

import com.alibaba.datax.common.util.Configuration;

public class RedisConfig {

    private Configuration core;
    private String host;
    private Integer port;
    private String password;
    private Integer pool_maxTotal;
    private Integer pool_maxIdle;
    private Integer pool_maxWaitMills;
    private Boolean pool_testOnBorrow;


    public RedisConfig(Configuration core){
        this.core = core;
        this.host = core.getString(RedisConstants.HOST, "localhost");
        this.port = core.getInt(RedisConstants.PORT, 6379);
        this.password = core.getString(RedisConstants.PASSWORD);
        this.pool_maxTotal = core.getInt(RedisConstants.POOL_MAX_TOTAL, 100);
        this.pool_maxIdle = core.getInt(RedisConstants.POOL_MAX_IDLE, 10);
        this.pool_maxWaitMills = core.getInt(RedisConstants.POOL_MAX_WAIT_MILLS, 10000);
        this.pool_testOnBorrow = core.getBool(RedisConstants.POOL_TEST_ON_BORROW, true);
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getPassword() {
        return password;
    }

    public Integer getPool_maxTotal() {
        return pool_maxTotal;
    }

    public Integer getPool_maxIdle() {
        return pool_maxIdle;
    }

    public Integer getPool_maxWaitMills() {
        return pool_maxWaitMills;
    }

    public Boolean getPool_testOnBorrow() {
        return pool_testOnBorrow;
    }

    interface RedisConstants{
        String HOST ="core.meta.redis.host";
        String PORT ="core.meta.redis.port";
        String PASSWORD ="core.meta.redis.password";
        String POOL_MAX_TOTAL ="core.meta.redis.pool.maxTotal";
        String POOL_MAX_IDLE ="core.meta.redis.pool.maxIdle";
        String POOL_MAX_WAIT_MILLS ="core.meta.redis.pool.maxWaitMills";
        String POOL_TEST_ON_BORROW ="core.meta.redis.pool.testOnBorrow";
    }
}
