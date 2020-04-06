package com.alibaba.datax.core.statistics.container.collector;

import java.util.Map;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public abstract class AbstractTGCollector implements TGCollector{
	
	
	public AbstractTGCollector(long jobId, int taskGroupId){
		this.jobId = jobId;
		this.taskGroupId = taskGroupId;
	}
	
	@Override
	public Map<Integer, Communication> getCommunicationMap() {
		return LocalTGCommunicationManager.getInstance().get(getJobId(), getTaskGroupId());
	}

	private Long jobId;
	private Integer taskGroupId;

    public Long getJobId() {
        return jobId;
    }

	public Integer getTaskGroupId() {
		return taskGroupId;
	}

}
