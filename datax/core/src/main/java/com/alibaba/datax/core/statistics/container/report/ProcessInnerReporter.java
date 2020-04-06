package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;

public class ProcessInnerReporter extends AbstractReporter {

	private long jobId;
	
	public ProcessInnerReporter(long jobId) {
		this.jobId = jobId;
	}
	
	
    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
    	LocalJobCommunicationManager.getInstance().updateJobCommunication(jobId, communication);
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        LocalJobCommunicationManager.getInstance().updateTaskGroupCommunication(this.jobId, taskGroupId, communication);
    }
}