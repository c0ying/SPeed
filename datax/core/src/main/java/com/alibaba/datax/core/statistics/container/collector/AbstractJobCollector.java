package com.alibaba.datax.core.statistics.container.collector;

public abstract class AbstractJobCollector implements JobCollector{
	
	public AbstractJobCollector(long jobId){
		this.jobId = jobId;
	}
	
	private Long jobId;

    public Long getJobId() {
        return jobId;
    }

}
