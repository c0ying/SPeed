package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.redis.RedisTGCommunicationManager;

public class RedisTGReporter extends AbstractTGReporter {

    private RedisTGCommunicationManager redisTGCommunicationManager;

    public RedisTGReporter(Long jobId, Integer taskGroupId, Configuration core) {
        super(jobId, taskGroupId);
        this.redisTGCommunicationManager = RedisTGCommunicationManager.newInstance(core);
    }

    @Override
    public void reportTGCommunication(Communication communication) {
        redisTGCommunicationManager.updateTaskGroupCommunication(jobId, taskGroupId, communication);
    }

    @Override
    public void reportTaskCommunication(Integer taskId, Communication communication) {
        redisTGCommunicationManager.updateTaskCommunication(jobId, taskGroupId, taskId, communication);
    }
}
