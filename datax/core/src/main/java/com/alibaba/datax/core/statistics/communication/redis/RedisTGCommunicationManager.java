package com.alibaba.datax.core.statistics.communication.redis;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.TGCommunicationManager;
import com.alibaba.datax.core.statistics.communication.redis.util.RedisClient;
import com.alibaba.datax.core.statistics.communication.redis.util.RedisClientProxy;
import com.alibaba.datax.core.statistics.communication.redis.util.bean.JsonCommunication;
import com.alibaba.datax.core.util.JsonUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang3.Validate;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class RedisTGCommunicationManager implements TGCommunicationManager {

    private static final String TG_TASK_KEY = "datax_tg_%s_%s";
    private static final String JOB_TG_KEY = "datax_job_%s_tg";
    private RedisClient redisClient;
    private RedisClientProxy redisClientProxy;
    private static RedisTGCommunicationManager redisTGCommunicationManager;

    private RedisTGCommunicationManager(Configuration core){
        this.redisClient = RedisClient.newInstance(core);
        this.redisClientProxy = new RedisClientProxy(core);
    }

    public static synchronized RedisTGCommunicationManager newInstance(Configuration core){
        if (redisTGCommunicationManager == null){
            redisTGCommunicationManager = new RedisTGCommunicationManager(core);
        }
        return redisTGCommunicationManager;
    }

    @Override
    public void register(long jobId, int taskGroupId, int task, Communication communication) {
        String key = String.format(TG_TASK_KEY, jobId, taskGroupId);
        Jedis jedis = redisClient.getJedis();
        communication.setState(State.WAITING);
        jedis.hset(key, String.valueOf(task), JsonUtil.toJson(JsonCommunication.build(communication)));
        jedis.close();
    }

    @Override
    public Map<Integer, Communication> getTaskCommunicationMap(long jobId, int taskGroupId) {
        String key = String.format(TG_TASK_KEY, jobId, taskGroupId);
        Jedis jedis = redisClient.getJedis();
        Map<String, String> allTGMap = jedis.hgetAll(key);
        jedis.close();
        Map<Integer, Communication> resultMap = new HashMap<>();
        allTGMap.forEach((k,v) -> {
            resultMap.put(Integer.parseInt(k), JsonUtil.parse(v, JsonCommunication.class).formate());
        });
        return resultMap;
    }

    @Override
    public Communication getTG(long jobId, int taskGroupId) {
        String key = String.format(JOB_TG_KEY, jobId);
        Jedis jedis = redisClient.getJedis();
        String tgJson = jedis.hget(key, String.valueOf(taskGroupId));
        jedis.close();
        return JsonUtil.parse(tgJson, JsonCommunication.class).formate();
    }

    @Override
    public void updateTaskCommunication(long jobId, int taskGroupId, int task, Communication communication) {
        String key = String.format(TG_TASK_KEY, jobId, taskGroupId);
        Jedis jedis = redisClient.getJedis();
        if(jedis.hexists(key, String.valueOf(task))){
            jedis.hset(key, String.valueOf(task), JsonUtil.toJson(JsonCommunication.build(communication)));
        }else{
            throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR, String.format("task[%s] not be registered", task));
        }
        jedis.close();
    }

    @Override
    public void updateTaskGroupCommunication(long jobId, int taskGroupId, Communication communication) {
        Communication oldCommunication = getTG(jobId, taskGroupId);
        if (oldCommunication != null){
            if (communication.getStartTimestamp() <= 0 && oldCommunication.getStartTimestamp() > 0) {
                communication.setStartTimestamp(oldCommunication.getStartTimestamp());
            }
            if (communication.getEndTimestamp() <= 0 && oldCommunication.getEndTimestamp() > 0) {
                communication.setEndTimestamp(oldCommunication.getEndTimestamp());
            }
            redisClientProxy.proxy(jedis -> {
                jedis.hset(String.format(JOB_TG_KEY, jobId), String.valueOf(taskGroupId), JsonUtil.toJson(JsonCommunication.build(communication)));
                return null;
            }, void.class);
        }else{
            Validate.isTrue(oldCommunication==null, String.format("taskGroupCommunicationMap中没有注册JobId[%d]的Communication，" +
                    "无法更新该taskGroup的信息", taskGroupId));
        }
    }

    @Override
    public void clearAll(long jobId, int taskGroupId) {
        String key = String.format(TG_TASK_KEY, jobId, taskGroupId);
        Jedis jedis = redisClient.getJedis();
        jedis.del(key);
        jedis.close();
    }

    @Override
    public void clearOne(long jobId, int taskGroupId, int task) {
        String key = String.format(TG_TASK_KEY, jobId, taskGroupId);
        Jedis jedis = redisClient.getJedis();
        jedis.hdel(key, String.valueOf(task));
        jedis.close();
    }
}
