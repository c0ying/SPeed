package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;

public class ProcessInnerJobReporter extends AbstractJobReporter {

    public ProcessInnerJobReporter(Long jobId) {
        super(jobId);
    }

    @Override
    public void reportJobCommunication(Communication communication) {
        LocalJobCommunicationManager.getInstance().updateJobCommunication(jobId, communication);
    }
}
