package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;

public abstract class AbstractJobReporter {

    protected long jobId;

    public AbstractJobReporter(Long jobId){
        this.jobId = jobId;
    }

    public long getJobId() {
        return jobId;
    }

    public abstract void reportJobCommunication(Communication communication);
}
