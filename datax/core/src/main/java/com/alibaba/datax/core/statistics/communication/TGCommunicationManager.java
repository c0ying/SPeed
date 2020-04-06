package com.alibaba.datax.core.statistics.communication;

import java.util.Map;

public interface TGCommunicationManager {

	void register(long jobId,int taskGroupId, int task, Communication communication);
	
	Map<Integer, Communication> get(long jobId, int taskGroupId);
	
	void clearAll(long jobId);
	
	void clearOne(long jobId, int taskGroupId);
}
