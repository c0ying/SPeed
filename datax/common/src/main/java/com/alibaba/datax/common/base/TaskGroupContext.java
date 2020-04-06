package com.alibaba.datax.common.base;

import java.util.concurrent.ConcurrentHashMap;

public class TaskGroupContext extends ConcurrentHashMap<String, Object>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8429711789227466568L;
	
	public static final String TASKGROUPINFO = "@TASKGROUPINFO@";

	public void putTaskGroupInfo(TaskGroupInfo taskGroupInfo) {
		put("@TASKGROUPINFO@", taskGroupInfo);
	}
	
	public TaskGroupInfo getTaskGroupInfo() {
		return (TaskGroupInfo) get(TASKGROUPINFO);
	}
}
