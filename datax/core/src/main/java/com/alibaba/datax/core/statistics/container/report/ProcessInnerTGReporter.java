package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerTGReporter extends AbstractTGReporter {

    public ProcessInnerTGReporter(Long jobId, Integer taskGroupId) {
        super(jobId, taskGroupId);
    }

    @Override
    public void reportTGCommunication(Communication communication) {
        LocalTGCommunicationManager.getInstance().updateTaskGroupCommunication(jobId, taskGroupId, communication);
    }

    @Override
    public void reportTaskCommunication(Integer taskId, Communication communication) {
        //do nothing
    }
}
