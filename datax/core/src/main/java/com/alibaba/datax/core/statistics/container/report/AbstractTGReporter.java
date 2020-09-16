package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.core.statistics.communication.Communication;

public abstract class AbstractTGReporter {

    protected long jobId;
    protected int taskGroupId;

    public AbstractTGReporter(Long jobId, Integer taskGroupId){
        this.jobId = jobId;
        this.taskGroupId = taskGroupId;
    }

    public long getJobId() {
        return jobId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    public abstract void reportTGCommunication(Communication communication);

    public abstract void reportTaskCommunication(Integer taskId, Communication communication);
}
