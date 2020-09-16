package com.alibaba.datax.core.statistics.communication;

import java.util.Map;

public interface TGCommunicationManager {

	void register(long jobId,int taskGroupId, int task, Communication communication);
	
	Map<Integer, Communication> getTaskCommunicationMap(long jobId, int taskGroupId);

	Communication getTG(long jobId, int taskGroupId);

	void updateTaskCommunication(long jobId,int taskGroupId, int task, Communication communication);

	void updateTaskGroupCommunication(long jobId,final int taskGroupId, final Communication communication);

	void clearAll(long jobId, int taskGroupId);
	
	void clearOne(long jobId, int taskGroupId, int task);
}
