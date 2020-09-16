package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.redis.RedisJobCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

import java.util.List;
import java.util.Map;

/**
 * TODO:后续需要再次完善，现TaskGroup汇报与Job汇报都直接向redis发送数据，
 * 应优化为TaskGroup发数据给Job汇报，再由Job向Redis汇报
 */
public class DistributeJobCollector extends AbstractJobCollector{

    private RedisJobCommunicationManager redisJobCommunicationManager;

    public DistributeJobCollector(Long jobId, Configuration core) {
        super(jobId);
        this.redisJobCommunicationManager = RedisJobCommunicationManager.newInstance(core);
    }

    @Override
    public Communication collectFromTaskGroup() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);
        Map<Integer, Communication> taskGroupCommunicationMap =
                redisJobCommunicationManager.getTaskGroupCommunicationMap(getJobId());
        if (taskGroupCommunicationMap != null) {
            for (Communication taskGroupCommunication : taskGroupCommunicationMap.values()) {
                communication.mergeFrom(taskGroupCommunication);
            }
        }
        return communication;
    }

    @Override
    public void registerJobCommunication() {

        redisJobCommunicationManager.registerJobCommunication(this.getJobId());
    }

    @Override
    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        for (Configuration config : taskGroupConfigurationList) {
            int taskGroupId = config.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            redisJobCommunicationManager.registerTaskGroupCommunication(this.getJobId(), taskGroupId, new Communication());
        }

    }

    @Override
    public Map<Integer, Communication> getTGCommunicationMap() {
        return redisJobCommunicationManager.getTaskGroupCommunicationMap(getJobId());
    }

    @Override
    public Communication getTGCommunication(Integer taskGroupId) {
        return redisJobCommunicationManager.getTaskGroupCommunication(getJobId(), taskGroupId);
    }

    @Override
    public Communication getJobCommunication() {
        return redisJobCommunicationManager.getJobCommunication(getJobId());
    }
}
