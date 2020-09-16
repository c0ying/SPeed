package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.redis.RedisJobCommunicationManager;

public class RedisJobReporter extends AbstractJobReporter{

    private RedisJobCommunicationManager redisJobCommunicationManager;

    public RedisJobReporter(Long jobId, Configuration core) {
        super(jobId);
        this.redisJobCommunicationManager = RedisJobCommunicationManager.newInstance(core);
    }

    @Override
    public void reportJobCommunication(Communication communication) {
        redisJobCommunicationManager.updateJobCommunication(jobId, communication);
    }
}
