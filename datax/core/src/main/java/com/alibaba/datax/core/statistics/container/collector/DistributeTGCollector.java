package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.redis.RedisTGCommunicationManager;
import com.alibaba.datax.core.statistics.communication.redis.util.RedisClientProxy;

import java.util.List;
import java.util.Map;

public class DistributeTGCollector extends ProcessInnerTGCollector {

    private static final String TASK_INFO = "datax_tg_%s_%s_task";
    private RedisTGCommunicationManager redisTGCommunicationManager;
    private RedisClientProxy redisClientProxy;

    public DistributeTGCollector(long jobId, int taskGroupId, Configuration core) {
        super(jobId, taskGroupId);
        this.redisTGCommunicationManager = RedisTGCommunicationManager.newInstance(core);
        this.redisClientProxy = new RedisClientProxy(core);
    }

    @Override
    public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
        super.registerTaskCommunication(taskConfigurationList);
        Map<Integer, Communication> communicationMap = this.getCommunicationMap();
        communicationMap.forEach((k,v) ->{
            redisTGCommunicationManager.register(getJobId(),getTaskGroupId(),k, v);
            redisClientProxy.proxy(jedis -> {
                if (!jedis.hexists(String.format(TASK_INFO, getJobId(), getTaskGroupId()), String.valueOf(k))) {
                    jedis.hset(String.format(TASK_INFO, getJobId(), getTaskGroupId()), String.valueOf(k), taskConfigurationList.get(k).toJSON());
                }
                return null;
            },void.class);

        });
    }

    @Override
    public Communication getTGCommunication() {
        return this.redisTGCommunicationManager.getTG(getJobId(), getTaskGroupId());
    }
}
