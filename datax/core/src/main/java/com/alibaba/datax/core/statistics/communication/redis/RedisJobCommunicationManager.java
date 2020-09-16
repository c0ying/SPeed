package com.alibaba.datax.core.statistics.communication.redis;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.JobCommunicationManager;
import com.alibaba.datax.core.statistics.communication.redis.util.RedisClientProxy;
import com.alibaba.datax.core.statistics.communication.redis.util.bean.JsonCommunication;
import com.alibaba.datax.core.util.JsonUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisJobCommunicationManager implements JobCommunicationManager {

    private RedisClientProxy redisClientProxy;
    private static final String JOB_KEY = "datax_job_%s";
    private static final String JOB_TG_KEY = "datax_job_%s_tg";
    private static RedisJobCommunicationManager redisJobCommunicationManager;

    private RedisJobCommunicationManager(Configuration core){
        this.redisClientProxy = new RedisClientProxy(core);
    }

    public static synchronized RedisJobCommunicationManager newInstance(Configuration core){
        if (redisJobCommunicationManager == null){
            redisJobCommunicationManager = new RedisJobCommunicationManager(core);
        }
        return redisJobCommunicationManager;
    }

    @Override
    public Communication registerJobCommunication(Long jobId) {
        return redisClientProxy.proxy(jedis -> {
            String jobCommunicationJson = jedis.get(String.format(JOB_KEY, jobId));
            if (StringUtils.isBlank(jobCommunicationJson)){
                Communication communication = new Communication();
                communication.setStartTimestamp(System.currentTimeMillis());
                communication.setState(State.SUBMITTING);
                jedis.set(String.format(JOB_KEY, jobId), JsonUtil.toJson(JsonCommunication.build(communication)));
                return communication;
            }
            return JsonUtil.parse(jobCommunicationJson, JsonCommunication.class).formate();
        }, Communication.class);
    }

    @Override
    public void clean(Long jobId) {
        redisClientProxy.proxy(jedis -> {
            jedis.del(String.format(JOB_KEY, jobId));
            jedis.del(String.format(JOB_TG_KEY, 1));
            return null;
        }, void.class);
    }

    @Override
    public void registerTaskGroupCommunication(long jobId, int taskGroupId, Communication communication) {
        redisClientProxy.proxy(jedis -> {
            if (!jedis.hexists(String.format(JOB_TG_KEY, jobId), String.valueOf(taskGroupId))){
                jedis.hset(String.format(JOB_TG_KEY, jobId), String.valueOf(taskGroupId), JsonUtil.toJson(JsonCommunication.build(communication)));
            }
            return null;
        },void.class);
    }

    @Override
    public Communication getJobCommunication(long jobId) {
        return redisClientProxy.proxy(jedis ->{
            String jobCommunicationJson = jedis.get(String.format(JOB_KEY, jobId));
            if (StringUtils.isNotBlank(jobCommunicationJson)){
                return JsonUtil.parse(jobCommunicationJson, JsonCommunication.class).formate();
            }else{
                return null;
            }
        }, Communication.class);
    }

    @Override
    public Set<Integer> getTaskGroupIdSet(long jobId) {
        return redisClientProxy.proxy(jedis -> {
            Map<String, String> tgCommunicationMap = jedis.hgetAll(String.format(JOB_TG_KEY, jobId));
            if (tgCommunicationMap != null){
                return tgCommunicationMap.keySet().stream().map(d -> Integer.parseInt(d)).collect(Collectors.toSet());
            }
            return null;
        }, Set.class);
    }

    @Override
    public Communication getTaskGroupCommunication(long jobId, int taskGroupId) {
        return redisClientProxy.proxy(jedis -> {
            String jsonCommunication = jedis.hget(String.format(JOB_TG_KEY, jobId), String.valueOf(taskGroupId));
            if (StringUtils.isNotBlank(jsonCommunication)){
                return JsonUtil.parse(jsonCommunication, JsonCommunication.class).formate();
            }
            return null;
        }, Communication.class);
    }

    @Override
    public Map<Integer, Communication> getTaskGroupCommunicationMap(long jobId) {
        return redisClientProxy.proxy(jedis -> {
            Map<String, String> tgCommunicationMap = jedis.hgetAll(String.format(JOB_TG_KEY, jobId));
            if (tgCommunicationMap != null){
                Map<Integer, Communication> result = new HashMap<>();
                tgCommunicationMap.entrySet().forEach(entry -> {
                    result.put(Integer.parseInt(entry.getKey()), JsonUtil.parse(entry.getValue(), JsonCommunication.class).formate());
                });
                return result;
            }
            return null;
        }, Map.class);
    }

    @Override
    public Set<Long> getAllJobId() {
        return redisClientProxy.proxy(jedis -> {
            Set<Long> jobIds = new HashSet<>();
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams();
            scanParams.match(String.format(JOB_KEY, "*"));
            scanParams.count(1000);
            while (true){
                //使用scan命令获取数据，使用cursor游标记录位置，下次循环使用
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();// 返回0 说明遍历完成
                List<String> list = scanResult.getResult();
                list.forEach(d->{
                    if (!d.endsWith("_tg")){
                        jobIds.add(Long.parseLong(d.replace(String.format(JOB_KEY, ""), "")));
                    }
                });
                if ("0".equals(cursor)){
                    break;
                }
            }
            return jobIds;
        }, Set.class);
    }

    @Override
    public void updateJobCommunication(long jobId, Communication communication) {
        Communication oldCommunication = getJobCommunication(jobId);
        if (oldCommunication != null){
            if (communication.getStartTimestamp() <= 0 && oldCommunication.getStartTimestamp() > 0) {
                communication.setStartTimestamp(oldCommunication.getStartTimestamp());
            }
            if (communication.getEndTimestamp() <= 0 && oldCommunication.getEndTimestamp() > 0) {
                communication.setEndTimestamp(oldCommunication.getEndTimestamp());
            }
            redisClientProxy.proxy(jedis -> {
                jedis.set(String.format(JOB_KEY, jobId), JsonUtil.toJson(JsonCommunication.build(communication)));
                return null;
            }, void.class);
        }else{
            Validate.isTrue(oldCommunication == null,
                    String.format("没有注册JobId[%d]的Communication，无法更新该Job的信息", jobId));

        }
    }
}
