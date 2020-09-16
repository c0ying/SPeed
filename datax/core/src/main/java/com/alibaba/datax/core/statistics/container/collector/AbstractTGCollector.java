package com.alibaba.datax.core.statistics.container.collector;

public abstract class AbstractTGCollector implements TGCollector{
	
	
	public AbstractTGCollector(long jobId, int taskGroupId){
		this.jobId = jobId;
		this.taskGroupId = taskGroupId;
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
