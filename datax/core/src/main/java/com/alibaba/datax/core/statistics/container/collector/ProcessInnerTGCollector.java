package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

import java.util.List;
import java.util.Map;

public class ProcessInnerTGCollector extends AbstractTGCollector {

    public ProcessInnerTGCollector(Long jobId, Integer taskGroupId) {
        super(jobId, taskGroupId);
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap() {
        return LocalTGCommunicationManager.getInstance().getTaskCommunicationMap(getJobId(), getTaskGroupId());
    }

	@Override
	public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
		long startTime = System.currentTimeMillis();
		for (Configuration taskConfig : taskConfigurationList) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            Communication taskCommunication = new Communication();
            taskCommunication.setStartTimestamp(startTime);
            LocalTGCommunicationManager.getInstance().register(getJobId(), getTaskGroupId(), taskId, taskCommunication);
        }
	}

	@Override
	public Communication collectFromTask() {
		Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :getCommunicationMap().values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
	}

	@Override
	public Communication getTaskCommunication(Integer taskId) {
		return getCommunicationMap().get(taskId);
	}

    @Override
    public Communication getTGCommunication() {
        return LocalJobCommunicationManager.getInstance().getTaskGroupCommunication(getJobId(), getTaskGroupId());
    }

}
