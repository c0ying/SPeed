package com.alibaba.datax.common.base;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;

public class TaskGroupInfo {

	private long jobId;
	private int taskGroupId;
	private List<Configuration> taskConfigs;
	
	public TaskGroupInfo(long jobId, int taskGroupId, List<Configuration> taskConfigs) {
		super();
		this.jobId = jobId;
		this.taskGroupId = taskGroupId;
		this.taskConfigs = taskConfigs;
	}
	public long getJobId() {
		return jobId;
	}
	public int getTaskGroupId() {
		return taskGroupId;
	}
	public List<Configuration> getTaskConfigs() {
		return taskConfigs;
	}
}
