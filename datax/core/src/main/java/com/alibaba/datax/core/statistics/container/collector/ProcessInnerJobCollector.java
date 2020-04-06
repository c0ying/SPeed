package com.alibaba.datax.core.statistics.container.collector;

import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

public class ProcessInnerJobCollector extends AbstractJobCollector {

    public ProcessInnerJobCollector(Long jobId) {
        super(jobId);
    }

    @Override
    public Communication collectFromTaskGroup() {
    	Communication communication = new Communication();
    	communication.setState(State.SUCCEEDED);
    	Map<Integer, Communication> taskGroupCommunicationMap = 
    			LocalJobCommunicationManager.getInstance().getTaskGroupCommunicationMap(getJobId());
    	if (taskGroupCommunicationMap != null) {
    		for (Communication taskGroupCommunication : taskGroupCommunicationMap.values()) {
    			communication.mergeFrom(taskGroupCommunication);
    		}
		}
        return communication;
    }

	@Override
	public void registerJobCommunication() {
		LocalJobCommunicationManager.getInstance().registerJobCommunication(getJobId());
	}

	@Override
	public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
		for (Configuration config : taskGroupConfigurationList) {
            int taskGroupId = config.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalJobCommunicationManager.getInstance().registerTaskGroupCommunication(getJobId(), taskGroupId, new Communication());
        }
		
	}

	@Override
	public Map<Integer, Communication> getTGCommunicationMap() {
		return LocalJobCommunicationManager.getInstance().getTaskGroupCommunicationMap(getJobId());
	}

	@Override
	public Communication getTGCommunication(Integer taskGroupId) {
		return LocalJobCommunicationManager.getInstance().getTaskGroupCommunication(getJobId(), taskGroupId);
	}

}
